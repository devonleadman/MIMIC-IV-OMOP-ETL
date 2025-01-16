from src import pipeline

while True:
    print("""
    1) START PIPELINE
    """)

    response = input()

    if response == "1":
        pipeline.start()
    else:
        print('Invalid Response')