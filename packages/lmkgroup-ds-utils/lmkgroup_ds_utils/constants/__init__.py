""" Stores constants commonly used in the code """


class Company:
    LMK = LINAS_MATKASSE = "6A2D0B60-84D6-4830-9945-58D518D27AC2"
    AMK = ADAMS_MATKASSE = "8A613C15-35E4-471F-91CC-972F933331D7"
    GL = GODTLEVERT = "09ECD4F0-AE58-4539-8E8F-9275B1859A19"
    RN = RETNEMT = "5E65A955-7B1A-446C-B24F-CFE576BF52D7"

    @classmethod
    def get_dict_variables(cls: type["Company"]) -> dict:
        return {key: value for key, value in cls.__dict__.items() if not key.startswith("__") and not callable(key)}


class ProductType:
    MEALBOX = "2F163D69-8AC1-6E0C-8793-FF0000804EB3"
    VELGVRAK = "CAC333EA-EC15-4EEA-9D8D-2B9EF60EC0C1"
