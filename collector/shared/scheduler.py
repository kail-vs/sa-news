def get_apis_to_run(minute: int):
    apis = []

    if minute % 15 == 0:
        apis.extend(["newsdatahub"])

    if minute % 2 == 0:
        apis.extend(["newsapi"])

    if minute % 8 == 0:
        apis.append("newsdata")

    return apis
