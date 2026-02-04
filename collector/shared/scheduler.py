def get_apis_to_run(minute: int):
    apis = []

    if minute % 15 == 0:
        apis.extend(["newsapi", "newsdatahub"])

    if minute % 10 == 0:
        apis.append("newsdata")

    return apis
