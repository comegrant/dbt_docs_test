webhook_urls = {
    "local_dev": (
        "https://hooks.slack.com/services/T1K1TQXPY/B07TU6NV47J/"
        "hNJmNAjTKlnUsAWYCSrg1Jnr"
    ),
    "dev": (
        "https://hooks.slack.com/services/T1K1TQXPY/B07U99J22F2/"
        "SmGFFBAEDjSRHHgbEqHp7kSf"
    ),
    "test": (
        "https://hooks.slack.com/services/T1K1TQXPY/B07TP0Y410C/"
        "HDVuYAONO2XQvKx01rPs22oZ"
    ),
    "prod": (
        "https://hooks.slack.com/services/T1K1TQXPY/B07TDSMD294/"
        "oBDzZLjzwOZLfu4pUfv32a0t"
    ),
}

databricks_workspace_urls = {
    "local_dev": (
        "https://adb-4291784437205825.5.azuredatabricks.net/"
        "jobs/runs?o=4291784437205825"
    ),
    "dev": (
        "https://adb-4291784437205825.5.azuredatabricks.net/"
        "jobs/runs?o=4291784437205825"
    ),
    "test": (
        "https://adb-3194696976104051.11.azuredatabricks.net/"
        "jobs/runs?o=3194696976104051"
    ),
    "prod": (
        "https://adb-3181126873992963.3.azuredatabricks.net/"
        "jobs/runs?o=3181126873992963"
    ),
}

power_bi_urls = {
    "local_dev": "https://app.powerbi.com/home?experience=power-bi",
    "dev": (
        "https://app.powerbi.com/groups/"
        "6ae6ab9c-e2a1-4b5d-8f4f-862a77d45721/list?experience=power-bi"
    ),
    "test": (
        "https://app.powerbi.com/groups/"
        "3a754e64-7ad7-4e6d-9d09-fb92b5a3606f/list?experience=power-bi"
    ),
    "prod": (
        "https://app.powerbi.com/groups/"
        "1c7d6f3e-2ed1-4b03-af86-ba852657e340/list?experience=power-bi"
    ),
}

github_url = "https://github.com/cheffelo/sous-chef"

slack_user_ids = {
    "stephen": "U05PZG6ED27",
    "stephen_test": "A05PZG6ED28",
    "marie": "U06CHPA1CLU",
    "anna": "U06B9U6R8D6",
    "sylvia": "U04TRPCUKGD",
    "mats": "U05Q1RUDC20",
    "agathe": "U07AVT7SG3D",
    "lina": "UF7UCANTC",
    "grant": "U058KUJC1PV",
    "synne": "U04E4HBEEHZ",
    "daniel": "UCJ8E7KE3",
}

slack_users = {
    "stephen": slack_user_ids["stephen"],
    "marie": slack_user_ids["marie"],
    "anna": slack_user_ids["anna"],
    "sylvia": slack_user_ids["sylvia"],
    "mats": slack_user_ids["mats"],
    "agathe": slack_user_ids["agathe"],
    "lina": slack_user_ids["lina"],
    "grant": slack_user_ids["grant"],
    "synne": slack_user_ids["synne"],
    "daniel": slack_user_ids["daniel"],
    "engineering": [
        slack_user_ids["anna"],
        slack_user_ids["marie"],
    ],
    "analytics": [
        slack_user_ids["lina"],
        slack_user_ids["daniel"],
        slack_user_ids["synne"],
        slack_user_ids["grant"],
    ],
    "science": [
        slack_user_ids["sylvia"],
        slack_user_ids["mats"],
        slack_user_ids["agathe"],
    ],
    "team": [
        slack_user_ids[name]
        for name in [
            "stephen",
            "marie",
            "anna",
            "sylvia",
            "mats",
            "agathe",
            "lina",
            "daniel",
            "synne",
            "grant",
        ]
    ]
}

slack_notification_job_ids = {
    "dev": 221967110790300,
    "test": 823927532277018,
    "prod": 808732355997444,
}
