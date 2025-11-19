import io

import backoff
import fsspec
import requests
from PIL import Image, ImageFilter
from sqlalchemy import exc
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
        stmt = select(
            WildCamerasAnnotationlabel.id,
            WildCamerasAnnotationlabel.text_,
            WildCamerasAnnotationlabel.blur,
        )
        res = s.exec(stmt)

    for row in res:
        label_map[row.text_] = row.id
        if row.blur:
            blur_labels.append(row.text_)

    return label_map, blur_labels


@backoff.on_exception(
    backoff.expo,
    (
        fsspec.exceptions.FSTimeoutError,
        FileNotFoundError,
    ),
    max_time=30,
)
def read_image_from_url(url, log) -> Image:
    with fsspec.open(url, mode="rb") as source_file:
        return Image.open(io.BytesIO(source_file.read()))


def get_or_create(
    session: Session, model, getter: dict, defaults
) -> tuple[SQLModel, bool]:
    stmt = select(model).where(getter)

    result = session.exec(stmt)
    try:
        return result.one(), False
    except exc.NoResultFound:
        obj = model(**defaults)
        session.add(obj)
        return obj, True


# based on https://gitlab.com/nina-data/viltkamera/viltkamera-blur/-/blob/main/viltkamera-blur.py


def blur_image(img: Image, bbox: WildCamerasBboxannotation, log) -> Image:
    """
    Blur an Image (PIL) given a BBoxAnnotation
    """
    log.debug("Blurring image...")
    width, height = img.size
    box = [
        round(number)
        for number in (
            bbox.x_min * width,
            bbox.y_min * height,
            bbox.x_max * width,
            bbox.y_max * height,
        )
    ]
    cropped = img.crop(box)
    blurred = cropped.filter(ImageFilter.GaussianBlur(50))
    img.paste(blurred, (box[0], box[1]))
    log.debug("done")
    return img
