import asyncio
import json
import uuid

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse

app = FastAPI()
active = 0
peak = 0


@app.get("/state")
async def state():
    return {"active": active, "peak": peak}


@app.post("/v1/messages")
async def messages(request: Request):
    global active, peak
    data = await request.json()
    text = str(data.get("messages", [{}])[-1].get("content", ""))
    delay = 2.0 if "slow" in text else 0.1
    status = 504 if "fail504" in text else 200
    msg = {
        "id": "msg_" + uuid.uuid4().hex,
        "type": "message",
        "role": "assistant",
        "model": "lab-model",
        "content": [{"type": "text", "text": "ok"}],
        "stop_reason": "end_turn",
        "stop_sequence": None,
        "usage": {"input_tokens": 10, "output_tokens": 2},
    }
    active += 1
    peak = max(peak, active)
    if not data.get("stream") or status != 200:
        try:
            await asyncio.sleep(delay)
            return JSONResponse(
                msg if status == 200 else {"error": {"type": "api_error", "message": "504 Gateway Timeout lab"}},
                status_code=status,
            )
        finally:
            active -= 1

    async def generate():
        global active

        def event(kind, value):
            return "event: " + kind + "\ndata: " + json.dumps({"type": kind, **value}) + "\n\n"

        try:
            yield event("message_start", {"message": {**msg, "content": [], "stop_reason": None}})
            yield event("content_block_start", {"index": 0, "content_block": {"type": "text", "text": ""}})
            await asyncio.sleep(delay)
            yield event("content_block_delta", {"index": 0, "delta": {"type": "text_delta", "text": "ok"}})
            yield event("content_block_stop", {"index": 0})
            yield event(
                "message_delta",
                {"delta": {"stop_reason": "end_turn", "stop_sequence": None}, "usage": {"output_tokens": 2}},
            )
            yield event("message_stop", {})
        finally:
            active -= 1

    return StreamingResponse(generate(), media_type="text/event-stream")


uvicorn.run(app, host="0.0.0.0", port=8080)
