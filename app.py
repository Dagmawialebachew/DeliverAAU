from fastapi import FastAPI, UploadFile, File
from fastapi.responses import FileResponse
from faster_whisper import WhisperModel
import os
import uuid

app = FastAPI()

# Use a more accurate model
model = None

@app.on_event("startup")
def load_model():
    global model
    model = WhisperModel(
        "base",  # start with base first
        device="cpu",
        compute_type="int8"
    )

UPLOAD_DIR = "uploads"
OUTPUT_DIR = "outputs"

os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)


def format_timestamp(seconds: float):
    millis = int(seconds * 1000)
    hours = millis // 3600000
    minutes = (millis % 3600000) // 60000
    seconds = (millis % 60000) // 1000
    milliseconds = millis % 1000
    return f"{hours:02}:{minutes:02}:{seconds:02},{milliseconds:03}"


@app.post("/transcribe")
async def transcribe(file: UploadFile = File(...)):
    file_id = str(uuid.uuid4())
    upload_path = os.path.join(UPLOAD_DIR, f"{file_id}_{file.filename}")

    with open(upload_path, "wb") as f:
        f.write(await file.read())

    # Transcribe with better accuracy settings
    segments, info = model.transcribe(
        upload_path,
        language="am",
        beam_size=5,           # better accuracy
        vad_filter=True        # cleaner segmentation
    )

    srt_path = os.path.join(OUTPUT_DIR, f"{file_id}.srt")

    with open(srt_path, "w", encoding="utf-8") as srt_file:
        for i, segment in enumerate(segments):
            start = format_timestamp(segment.start)
            end = format_timestamp(segment.end)
            text = segment.text.strip()

            srt_file.write(f"{i+1}\n")
            srt_file.write(f"{start} --> {end}\n")
            srt_file.write(f"{text}\n\n")

    return FileResponse(
        srt_path,
        media_type="application/x-subrip",
        filename="subtitles.srt"
    )