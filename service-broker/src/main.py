import sys
import uvicorn


from config import Config

Config("config.yaml")

if __name__ == "__main__":
    port = 9904
    try:
        if len(sys.argv) == 2:
            port = int(sys.argv[1])
        print(f"server port: {port}")
    except ValueError:
        print("argument error, so set 9904 as the default port.", sys.stderr)
        port = 9904

    # run fastapi server using uvicorn
    uvicorn.run(
        "api.api_main:fast_api",
        host="0.0.0.0",
        port=port,
        log_level="info",
        reload=True,
        timeout_keep_alive=3600,
    )
