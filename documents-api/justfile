dev:
    NAVIGATOR_DATABASE_URL=postgresql://navigator:navigator@localhost/navigator poetry run fastapi dev app/main.py --port 8080

requirements:
    poetry export --without-hashes --without-urls --format requirements.txt | sed 's/ ;.*//'  > requirements.txt
