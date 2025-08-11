from PIL import Image

def remove_alpha(image, **kwargs):
    """Заменяет прозрачность на белый фон (для PNG, WEBP и др.)"""
    if image.mode in ("RGBA", "LA") or (image.mode == "P" and "transparency" in image.info):
        bg = Image.new("RGB", image.size, (202, 202, 202))
        bg.paste(image, mask=image.split()[-1])  # Use alpha channel as mask
        return bg
    return image
