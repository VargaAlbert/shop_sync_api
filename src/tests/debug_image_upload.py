from __future__ import annotations

import os
from dotenv import load_dotenv

from src.shoprenter.client import ShoprenterClient
from src.utils.images import prepare_shoprenter_image_upload

load_dotenv()


def main():

    # TESZT KÉP
    image_url = "https://www.haldepo.hu/uploads/media/617bf005150d8/149088-001.jpg"

    supplier = "haldepo"
    sku = "2954"
    model = "149088-001"

    prepared = prepare_shoprenter_image_upload(
        supplier_name=supplier,
        image_url=image_url,
        sku=sku,
        model=model,
    )

    print("FILE PATH:", prepared["file_path"])
    print("BASE64 SIZE:", len(prepared["base64_data"]))
    print("BASE64 HEAD:", prepared["base64_data"][:80])

    print("SHOPRENTER_API_URL =", os.getenv("SHOPRENTER_API_URL"))
    #print("SHOPRENTER_API_USER =", os.getenv("SHOPRENTER_API_USER"))
    #print("SHOPRENTER_API_PASS =", "***" if os.getenv("SHOPRENTER_API_PASS") else None)

    client = ShoprenterClient(
        base_url=os.getenv("SHOPRENTER_API_URL"),
        user=os.getenv("SHOPRENTER_API_USER"),
        password=os.getenv("SHOPRENTER_API_PASS"),
    )

    resp = client.upload_file(
        file_path=prepared["file_path"],
        base64_content=prepared["base64_data"],
        file_type="image",
    )

    print("UPLOAD RESPONSE:")
    print(resp)


if __name__ == "__main__":
    main()