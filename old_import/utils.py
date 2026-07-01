from typing import TypeVar

import backoff
import fsspec
import fsspec.exceptions
import requests
import requests.exceptions
from PIL import Image, ImageFilter
from sqlalchemy import ColumnElement, exc
from sqlmodel import Session, SQLModel, select

from .models import WildCamerasAnnotationlabel, WildCamerasBboxannotation


@backoff.on_exception(
    backoff.expo,
    (
        requests.exceptions.Timeout,
        requests.exceptions.ConnectionError,
        requests.exceptions.HTTPError,
    ),
    max_time=60 * 5,
)
def get_http_session(base_url, username, password, log):
    url = f"{base_url}/login"
    values = {"username": username, "password": password}

    log.debug("Trying to authenticate", username=username)

    session = requests.Session()
    response = session.post(url, json=values)
    log.debug("response", response=response.text)
    response.raise_for_status()

    cookies = requests.utils.dict_from_cookiejar(session.cookies)

    log.debug("login successful", cookies=cookies)
    return {"cookies": cookies}


def get_labels(engine) -> tuple[dict[str, int], list[str]]:
    label_map = {}
    blur_labels = []

    with Session(engine) as s:
        res = s.exec(select(WildCamerasAnnotationlabel)).all()

    for row in res:
        label_map[row.text_] = row.id
        if row.blur:
            blur_labels.append(row.text_)

    return label_map, blur_labels


@backoff.on_exception(
    backoff.expo,
    fsspec.exceptions.FSTimeoutError,
    max_time=30,
)
def download_raw_bytes(url: str, log) -> bytes:
    with fsspec.open(url, mode="rb") as source_file:
        return source_file.read()


T = TypeVar("T", bound=SQLModel)


def get_or_create(
    session: Session, model: type[T], getter: ColumnElement[bool], defaults: dict
) -> tuple[T, bool]:
    stmt = select(model).where(getter)

    result = session.exec(stmt)
    try:
        return result.one(), False
    except exc.NoResultFound:
        obj = model(**defaults)
        session.add(obj)
        return obj, True


# based on https://gitlab.com/nina-data/viltkamera/viltkamera-blur/-/blob/main/viltkamera-blur.py


def blur_image(img: Image.Image, bbox: WildCamerasBboxannotation, log) -> Image.Image:
    """
    Blur an Image (PIL) given a BBoxAnnotation
    """
    log.debug("Blurring image...")
    width, height = img.size
    x_min, y_min, x_max, y_max = bbox.x_min, bbox.y_min, bbox.x_max, bbox.y_max
    if x_min is None or y_min is None or x_max is None or y_max is None:
        raise ValueError(f"BBox {bbox.id} has None coordinates, cannot blur")
    box = [
        round(number)
        for number in (
            x_min * width,
            y_min * height,
            x_max * width,
            y_max * height,
        )
    ]
    cropped = img.crop(box)
    blurred = cropped.filter(ImageFilter.BoxBlur(50))
    img.paste(blurred, (box[0], box[1]))
    log.debug("done")
    return img
