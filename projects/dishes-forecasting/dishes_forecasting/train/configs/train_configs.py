def get_training_configs(
    company_code: str
) -> dict:
    if company_code == "GL":
        train_config = train_config_gl
    elif company_code == "AMK":
        train_config = train_config_amk
    elif company_code == "LMK":
        train_config = train_config_lmk
    elif company_code == "RT":
        train_config = train_config_rt
    return train_config


train_config_gl = {
    "train_start_yyyyww": 202101,
    "train_end_yyyyww": 205001
}

train_config_amk = {
    "train_start_yyyyww": 202101,
    "train_end_yyyyww": 205001
}

train_config_lmk = {
    "train_start_yyyyww": 202101,
    "train_end_yyyyww": 205001
}

train_config_rt = {
    "train_start_yyyyww": 202201,
    "train_end_yyyyww": 205001
}
