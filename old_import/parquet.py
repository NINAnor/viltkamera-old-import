import json
from collections import defaultdict
from datetime import datetime

import duckdb
from dateutil import tz
from dateutil.parser import parse
from sqlmodel import Session, col, delete, select

from .models import (
    WildCamerasBboxannotation,
    WildCamerasDataset,
    WildCamerasImage,
    WildCamerasLocation,
    WildCamerasTimeseries,
    WildCamerasValidationrevision,
)
from .utils import blur_image, get_or_create, read_image_from_url


def resolve_camera_id(raw: str) -> int | None:
    raw = raw.strip()
    if raw.isdigit():
        return int(raw)
    base = raw.split("_")[0]
    if base.isdigit():
        return int(base)
    return None


def get_dataset_by_id(
    dataset_id: str | int,
    project_path: str,
    timeseries_path: str,
    image_path: str,
    connection: duckdb.DuckDBPyConnection,
    engine,
    label_map: tuple[dict[str, int], list[str]],
    image_source_path: str,
    image_target_path: str,
    log,
    s3,
    single=False,
) -> bool:
    log = log.bind(dataset_id=dataset_id)

    datasets = (
        connection.execute(
            f"from read_parquet('{project_path}') where id = $1 limit 1",  # noqa: E501
            [dataset_id],
        )
        .fetch_arrow_table()
        .to_pylist()
    )
    try:
        ds = datasets[0]
    except IndexError:
        log.error("Dataset not found", dataset_id=dataset_id)
        return False

    if ds.get("status") == "failed":
        log.warning("Skipping failed dataset", dataset_id=dataset_id)
        return True

    with Session(engine) as s:
        log.debug("Get location of this dataset")

        camera_id = resolve_camera_id(str(ds["camera_id"]))
        if camera_id is None:
            log.warning(
                "Skipping non-numeric camera_id",
                camera_id=str(ds["camera_id"]),
                dataset_id=dataset_id,
            )
            return True
        location, created = get_or_create(
            session=s,
            model=WildCamerasLocation,
            getter=col(WildCamerasLocation.id) == camera_id,
            defaults={
                "id": camera_id,
                "created_at": datetime.now(tz.UTC),
                "last_modified_at": datetime.now(tz.UTC),
            },
        )

        log.debug("done", object=location, created=created)

        s.flush()

        log.debug("Create the dataset")
        dataset, created = get_or_create(
            session=s,
            model=WildCamerasDataset,
            getter=col(WildCamerasDataset.ext_id) == int(ds["id"]),
            defaults={
                "locked": True,
                "comment": "",
                "created_at": parse(ds["created_at"]),
                "last_modified_at": parse(ds["updated_at"]),
                "location": location,
                "deleted": ds["deleted"],
                "registration_id": None,
                "delta": None,
                "ext_id": ds["id"],
            },
        )
        log.debug("done", object=dataset, created=created)

        s.flush()
        dataset_db_id = dataset.id

        if not created:
            old_ids = s.scalars(
                select(WildCamerasTimeseries.ext_id).where(
                    WildCamerasTimeseries.dataset_id == dataset_db_id,
                    col(WildCamerasTimeseries.ext_id).is_not(None),
                )
            ).all()
        else:
            old_ids = []

        # TODO: get the ids of the already loaded timeseries
        timeseries_set = (
            connection.execute(
                f"""
                    from read_parquet('{timeseries_path}')
                        select *,
                    where id in (
                        from read_parquet('{project_path}')
                        select unnest(timeseries) as id
                        where id = $1
                    ) and id not in $2
                    order by id
                    """,  # noqa: E501, S608
                [dataset_id, old_ids],
            )
            .fetch_arrow_table()
            .to_pylist()
        )

        log.debug("found %s timeseries", len(timeseries_set))

        if not timeseries_set:
            if created:
                log.info("Dataset has no timeseries, skipping")
            return True

        s.commit()
        log.debug("committed", dataset_db_id=dataset_db_id)

    # Pre-fetch images for all new timeseries in one DuckDB scan
    timeseries_ids = [ts["id"] for ts in timeseries_set]
    images_by_timeseries = defaultdict(list)

    if timeseries_ids:
        ts_images = (
            connection.execute(
                f"""
                    select id, unnest(images) as image_id,
                           generate_subscripts(images, 1) AS image_index,
                           selected_image
                    from read_parquet('{timeseries_path}')
                    where id in ({", ".join("?" for _ in timeseries_ids)})
                """,  # noqa: S608
                timeseries_ids,
            )
            .fetch_arrow_table()
            .to_pylist()
        )
        all_image_ids = list({r["image_id"] for r in ts_images})

        if all_image_ids:
            image_records = (
                connection.execute(
                    f"""
                        select * exclude (ground_truth_label, ground_truth_boxes)
                        from read_parquet('{image_path}')
                        where id in ({", ".join("?" for _ in all_image_ids)})
                    """,  # noqa: S608
                    all_image_ids,
                )
                .fetch_arrow_table()
                .to_pylist()
            )
            image_map = {img["id"]: img for img in image_records}

            for r in ts_images:
                img_data = image_map.get(r["image_id"])
                if img_data:
                    combined = {
                        **img_data,
                        "image_index": r["image_index"],
                        "selected_image": r["selected_image"],
                    }
                    images_by_timeseries[r["id"]].append(combined)

            for imgs in images_by_timeseries.values():
                imgs.sort(key=lambda x: x["image_index"])

    images_ok = 0
    errors = 0
    for ts in timeseries_set:
        with Session(engine) as session:
            log.debug("getting timeseries images")
            log = log.bind(timeseries=ts["id"])

            log.debug("getting timeseries")
            timeseries, created = get_or_create(
                session=session,
                model=WildCamerasTimeseries,
                getter=col(WildCamerasTimeseries.ext_id) == ts["id"],
                defaults={
                    "created_at": parse(ts["created_at"]),
                    "last_modified_at": parse(ts["updated_at"]),
                    "comment": "",
                    "hidden": ts["predicted_label"] == "nothing",
                    "predicted_species_id": label_map[0][ts["predicted_label"]],
                    "validated_species_id": label_map[0][ts["ground_truth_label"]]
                    if ts["ground_truth_label"]
                    else None,
                    "extra": {
                        "distance": ts["distance"],
                        "num_animals": ts["num_animals"],
                        "should_export_images": ts["should_export_images"],
                        "camera_inactive": ts["camera_inactive"],
                        "taken_offset": ts["taken_offset"],
                    },
                    "ext_id": ts["id"],
                    "dataset_id": dataset_db_id,
                },
            )
            log.debug("done", object=timeseries, created=created)
            if created:
                session.flush()
                log.debug("missing, starting to populate it")

                if ts["status"] == "verified":
                    log.debug('status is "verified, adding a revision')
                    # only verified timeseries should be added in the revision
                    timeseries.revisions.append(
                        WildCamerasValidationrevision(
                            comment="",
                            created_at=ts["updated_at"],
                            last_modified_at=ts["updated_at"],
                            label_id=label_map[0][ts["ground_truth_label"]],
                            # NOTE: there is no info about the user that reviewed,
                            # so by default we'll use -1
                            user_id=-1,
                        )
                    )
                try:
                    process_timeseries(
                        session=session,
                        dataset_id=dataset_db_id,
                        image_source_path=image_source_path,
                        image_target_path=image_target_path,
                        label_map=label_map,
                        timeseries=timeseries,
                        log=log,
                        s3=s3,
                        images=images_by_timeseries.get(ts["id"], []),
                    )
                    session.commit()
                    images_ok += len(images_by_timeseries.get(ts["id"], []))

                except (KeyboardInterrupt, duckdb.InterruptException) as e:
                    raise KeyboardInterrupt from e
                except Exception as e:
                    log.warning("error", timeseries=ts["id"], error=str(e))
                    errors += 1

                if single:
                    break
            else:
                log.debug("already present, skipping")

    log.info(
        "Import complete",
        dataset_id=dataset_id,
        timeseries=len(timeseries_set),
        images=images_ok,
        errors=errors,
    )
    return True


def process_timeseries(
    timeseries: WildCamerasTimeseries,
    session: Session,
    dataset_id: int,
    label_map: tuple[dict[str, int], list[str]],
    image_source_path: str,
    image_target_path: str,
    log,
    s3,
    images: list[dict],
):
    log.debug("found %s images", len(images))

    for i in images:
        file_path = f"processed/tsimages/imported/{i['id']}"

        pil_image = read_image_from_url(url=f"{image_source_path}{i['id']}", log=log)

        image = WildCamerasImage(
            uuid=i["id"],
            ext_id=i["id"],
            metadata_=json.loads(i["exif"]),
            captured_at=i["taken_at"],
            dataset_id=dataset_id,
            classified_at=parse(i["predicted_at"]),
            # TODO: set the file path
            file=file_path,
            # Naively set hidden on images predicted as
            hidden=i["predicted_label"] == "nothing",
            timeseries=timeseries,
            sequence_index=i["image_index"],
        )
        session.add(image)
        session.flush()

        log = log.bind(image=image.uuid)
        log.debug("created image")

        if i["id"] == i["selected_image"]:
            session.refresh(timeseries)
            session.refresh(image)
            timeseries.selected_image = image
            session.add(timeseries)
            log.debug("set image as selected")

        log.debug("populating bboxes")
        for bbox in i["predicted_boxes"]:
            session.refresh(image)
            box = WildCamerasBboxannotation(
                created_at=image.classified_at,
                score=bbox["score"],
                x_max=bbox["box"]["xmax"],
                y_max=bbox["box"]["ymax"],
                x_min=bbox["box"]["xmin"],
                y_min=bbox["box"]["ymin"],
                label_id=label_map[0][bbox["label"]],
                user_id=-1,
            )
            image.bboxes.append(box)

            if bbox["label"] in label_map[1]:
                log.debug("applying blur on bbox", label=bbox["label"])
                pil_image = blur_image(pil_image, log=log, bbox=box)

        target = image_target_path + file_path
        log.debug("Saving image to s3", target=target)
        pil_image.save(s3.open(target, "wb"), format="jpeg")
        log.debug("done")


def clean_dataset(session, dataset_id):
    stmts = [
        delete(WildCamerasBboxannotation).where(
            WildCamerasBboxannotation.image_id == WildCamerasImage.id,
            WildCamerasImage.dataset_id == WildCamerasDataset.id,
            WildCamerasDataset.ext_id == dataset_id,
        ),
        delete(WildCamerasImage).where(
            WildCamerasImage.dataset_id == WildCamerasDataset.id,
            WildCamerasDataset.ext_id == dataset_id,
        ),
        delete(WildCamerasValidationrevision).where(
            WildCamerasValidationrevision.timeseries_id == WildCamerasTimeseries.id,
            WildCamerasTimeseries.dataset_id == WildCamerasDataset.id,
            WildCamerasDataset.ext_id == dataset_id,
        ),
        delete(WildCamerasTimeseries).where(
            WildCamerasTimeseries.dataset_id == WildCamerasDataset.id,
            WildCamerasDataset.ext_id == dataset_id,
        ),
        delete(WildCamerasDataset).where(WildCamerasDataset.ext_id == dataset_id),
    ]

    for stmt in stmts:
        session.exec(stmt)
