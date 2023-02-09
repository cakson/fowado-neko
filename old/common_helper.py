import io
import tempfile

import PIL
import imageio
import numpy as np
from PIL import Image
import cv2


def convert_to_mb(bytes_val):
    return bytes_val / (1024 * 1024)


def get_screenshot(file):
    try:
        file.seek(0)

        with tempfile.NamedTemporaryFile(delete=True) as temp_file:
            temp_file.write(file.read())
            temp_file_path = temp_file.name

            # Read video from the temporary file
            video = cv2.VideoCapture(temp_file_path)

            # Read the first frame from the video
            success, frame = video.read()

            # Check if the frame was read successfully
            if success:
                # Convert the frame to a numpy array
                frame = np.array(frame)

                # Convert the color from BGR to RGB
                frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)

            # Release the video capture object
            video.release()

            # Convert the numpy array to a PIL image
            img = Image.fromarray(frame)

            # Save the PIL image to an io.BytesIO object
            screenshot = io.BytesIO()
            img.save(screenshot, format='JPEG')

            return screenshot

    except Exception as e:
        print(e)

    return None
