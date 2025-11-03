import json
from datetime import datetime

import duckdb
import tqdm
from dateutil import tz
from dateutil.parser import parse
from sqlmodel import Session, delete, select

from .models import (
    WildCamerasBboxannotation,
    WildCamerasDataset,
    WildCamerasImage,
    WildCamerasLocation,
    WildCamerasTimeseries,
    WildCamerasValidationrevision,
)
from .utils import blur_image, get_http_session, get_or_create, read_image_from_url


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
    api_user,
    api_password,
    api_base,
    s3,
    single=False,
):
    log = log.bind(dataset_id=dataset_id)

    http_config = get_http_session(api_base, api_user, api_password, log)

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
        return
    with Session(engine) as s:
        log.debug("Get location of this dataset")

        location, created = get_or_create(
            session=s,
            model=WildCamerasLocation,
            getter=WildCamerasLocation.id == int(ds["camera_id"]),
            defaults={
                "id": ds["camera_id"],
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
            getter=WildCamerasDataset.ext_id == int(ds["id"]),
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

        s.commit()
        dataset_db_id = dataset.id
        log.debug("committed", dataset_db_id=dataset_db_id)

        if not created:
            old_ids = s.scalars(
                select(WildCamerasTimeseries.ext_id).where(
                    WildCamerasTimeseries.dataset_id == dataset_db_id,
                    WildCamerasTimeseries.ext_id is not None,
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

    for ts in tqdm.tqdm(timeseries_set):
        with Session(engine) as session:
            log.debug("getting timeseries images")
            log = log.bind(timeseries=ts["id"])

            log.debug("getting timeseries")
            timeseries, created = get_or_create(
                session=session,
                model=WildCamerasTimeseries,
                getter=WildCamerasTimeseries.ext_id == ts["id"],
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
                        connection=connection,
                        dataset_id=dataset_db_id,
                        timeseries_path=timeseries_path,
                        image_path=image_path,
                        image_source_path=image_source_path,
                        image_target_path=image_target_path,
                        label_map=label_map,
                        timeseries=timeseries,
                        log=log,
                        http_config=http_config,
                        s3=s3,
                    )
                    session.commit()

                except (
                    KeyboardInterrupt,
                    duckdb.InterruptException,
                    RuntimeError,
                ) as e:
                    raise KeyboardInterrupt from e
                except Exception as e:
                    log.exception(e)

                if single:
                    break
            else:
                log.debug("already present, skipping")


def process_timeseries(
    timeseries: WildCamerasTimeseries,
    session: Session,
    connection: duckdb.DuckDBPyConnection,
    timeseries_path: str,
    image_path: str,
    dataset_id: int,
    label_map: tuple[dict[str, int], list[str]],
    image_source_path: str,
    image_target_path: str,
    log,
    http_config,
    s3,
):
    images = (
        connection.execute(
            f"""
                with timeseries_image as (
                    from read_parquet('{timeseries_path}/timeseries.parquet/*')
                        select selected_image,
                        unnest(images) as image_id,
                        generate_subscripts(images, 1) AS image_index,
                    where id = $1
                )
                select
                    img.* exclude (ground_truth_label, ground_truth_boxes),
                    tsi.image_index as image_index,
                    tsi.selected_image as selected_image,
                from timeseries_image as tsi
                join read_parquet('{image_path}/images_metadata.parquet/*') as img on tsi.image_id = img.id
                order by image_index
                """,  # noqa: E501, S608
            [timeseries.ext_id],
        )
        .fetch_arrow_table()
        .to_pylist()
    )
    log.debug("found %s images", len(images))

    for i in images:
        file_path = f"processed/tsimages/imported/{i['id']}"

        pil_image = read_image_from_url(
            url=f"{image_source_path}{i['id']}", http_config=http_config, log=log
        )

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
