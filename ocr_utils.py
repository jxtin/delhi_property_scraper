from io import BytesIO

import pytesseract
import requests
from PIL import Image, ImageFilter, ImageOps


def get_image(url):
    response = requests.get(url)
    img = Image.open(BytesIO(response.content))
    return img


def process_image(img):
    prebin = img
    postbin = img
    for i in range(3):
        prebin = prebin.filter(ImageFilter.MedianFilter())
    prebin = ImageOps.grayscale(prebin)
    prebin = prebin.filter(ImageFilter.SHARPEN)
    prebin = prebin.filter(ImageFilter.MaxFilter(3))
    prebin = prebin.filter(ImageFilter.SHARPEN)
    postbin = prebin.point(lambda x: 0 if x < 200 else 255, "1")
    return prebin, postbin


def ocr(img):
    return pytesseract.image_to_string(
        img,
        config="--psm 13 --oem 3 -c tessedit_char_whitelist=ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789",
    ).strip()


def solve_captcha(url):
    img = get_image(url)
    prebin, postbin = process_image(img)
    ocr_img, ocr_prebin, ocr_postbin = ocr(img), ocr(prebin), ocr(postbin)

    if not any(len(x) == 5 for x in [ocr_img, ocr_prebin, ocr_postbin]):
        print("could not solve captcha")
        return solve_captcha(url)
    else:
        return ocr_img, ocr_prebin, ocr_postbin
