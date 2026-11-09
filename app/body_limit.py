from fastapi import HTTPException, Request
from fastapi.routing import APIRoute


MAX_JOB_BODY_BYTES = 4096


class JobBodyLimitRoute(APIRoute):
    def get_route_handler(self):
        handle_job = super().get_route_handler()

        async def handle_limited_job(request: Request):
            received_bytes = 0

            async def receive_limited():
                nonlocal received_bytes
                message = await request.receive()
                if message["type"] == "http.request":
                    received_bytes += len(message.get("body", b""))
                    if received_bytes > MAX_JOB_BODY_BYTES:
                        # FastAPI handles this while reading the body, before JSON parsing.
                        raise HTTPException(status_code=413, detail="Job payload too large")
                return message

            return await handle_job(Request(request.scope, receive=receive_limited))

        return handle_limited_job
