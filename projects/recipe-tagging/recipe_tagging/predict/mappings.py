"""
Mapping of recipe data keywords to SEO taxonomy types,
and penalty words for dish names and proteins.

The mappings include:
- preference mapping: Maps dietary preferences.
- protein category mapping: Maps protein categories (meat, fish, etc.).
- trait mapping: Maps traits (easy to cook, quick to prepare).
- dish mapping: Maps dish names (burger, soup, etc.).
- protein mapping: Maps protein names (alaska pollock, anchovies, etc.).
- cuisine mapping: Maps cuisine names (italian, french, etc.), specific to company languages.
"""

preference_mapping: dict[str, dict[str, list[str]]] = {
    "main_ingredient": {
        "Vegetarian": [
            "vegetar",
            "vegetariskt",
        ],
        "Vegan": [
            "vegan",
        ],
    },
    "negative_preference": {
        "Lactose Free": [
            "lactose",
        ],
        "Gluten Free": [
            "gluten",
        ],
        "Pork Free": [
            "pork",
        ],
        "Egg Free": [
            "egg",
        ],
    },
    "column": {
        "Low Calorie": [
            "is_low_calorie",
        ],
        "High Fiber": [
            "is_high_fiber",
        ],
        "Low Fat": [
            "is_low_fat",
        ],
        "Low Sugar": [
            "is_low_sugar",
        ],
    },
}

protein_category_mapping: dict[str, dict[str, list[str]]] = {
    "main_ingredient": {
        "Meat": [
            "svin",
            "fläsk",
            "svinekød",
            "storfe",
            "nötkött",
            "oksekød",
            "lam",
            "lamm",
            "kjøtt",
            "kött",
            "kød",
            "vilt",
            "vildt",
            "pølse",
            "korv/chark",
            "kalvekød",
            "kalv/svin",
        ],
        "Seafood": [
            "fisk",
            "skalldyr",
            "skaldjur",
            "skaldyr",
        ],
        "Fish": [
            "fisk",
        ],
        "Bird": [
            "fugl",
            "fågel",
            "fjerkræ",
            "kalkun",
            "kylling",
            "and",
        ],
    },
    "ingredient": {
        "Tofu": [
            "tofu",
        ],
        "Quorn": [
            "quorn",
        ],
        "Omph": [
            "oumph",
            "omph",
        ],
    },
}

trait_mapping: dict[str, dict[str, list[str]]] = {
    "taxonomy": {
        "Child Friendly": ["family"],
    },
    "column": {
        "Easy To Cook": ["cooking_time_to"],
        "Quick To Prepare": ["cooking_time_to"],
    },
    "ingredient": {
        "Spicy": ["spicy", "chili", "sterk", "stark", "stærk"],
    },
}

dish_mapping: dict[str, dict[str, list[str]]] = {
    "recipe_name": {
        "Burger": [
            "burger",
            "burgar",
        ],
        "Gratin": [
            "grateng",
            "gratäng",
            "gratin",
        ],
        "Pie": [
            "pai",
            "paj",
            "tærte",
            "pie",
        ],
        "Pizza": [
            "pizza",
        ],
        "Soup": [
            "suppe",
            "soppa",
        ],
        "Stew": [
            "gryterett",
            "ragu",
            "gryte",
            "lapskaus",
            "gryta",
            "gryderet",
            "gryde",
            "simre",
            "ragout",
        ],
    }
}

protein_mapping: dict[str, dict[str, list[str]]] = {
    "ingredient": {
        "Alaska Pollock": [
            "alaska pollock",
            "alaskasej",
            "alaskatorsk",
        ],
        "Anchovies": [
            "ansjos",
            "ansjovis",
        ],
        "Anchovy": [
            "ansjos",
            "ansjovis",
        ],
        "Arctic Char": [
            "røye",
            "rødding",
            "röding",
        ],
        "Bacon": [
            "bacon",
        ],
        # "Baltic Herring": [
        #    "Østersjøsild",
        #    "Strömming",
        #    "Østersøsild",
        # ],
        "Beef": [
            "storfe",
            "nötkött",
            "oksekød",
        ],
        "Beef Tenderloin": [
            "oxfilé",
            "indrefilet Av Okse",
            "oksemørbrad",
        ],
        "Brisket": [
            "bringa",
            "spidsbryst",
            "bibringe",
        ],
        # "Cattle": [
        #     "kveg",
        #     "kvæg",
        #     "nötkreatur",
        # ],
        "Caviar": [
            "kaviar",
        ],
        "Chicken": [
            "kylling",
            "kyckling",
        ],
        "Cocktail Sausage": [
            "cocktailpølse",
            "cocktailkorv",
        ],
        "Cod": [
            "torsk",
        ],
        "Crab": [
            "krabbe",
            "krabba",
        ],
        "Crayfish": [
            "kreps",
            "kräfta",
            "krebs",
        ],
        "Fillet": [
            "filea",
            "filet",
        ],
        "Flank Steak": [
            "flankstek",
            "flanksteak",
            "flankestek",
        ],
        "Flounder": [
            "flyndre",
            "flundra",
            "skrubbe",
        ],
        "Game Meat": [
            "vildtkød",
            "viltkött",
            "viltkjøtt",
        ],
        "Goose": [
            "gås",
        ],
        "Ground Meat": [
            "kjøttdeig",
            "hakket kød",
            "malet kött",
        ],
        "Haddock": [
            "kuller",
            "kolja",
            "hyse",
        ],
        "Hake": [
            "kummel",
            "lysing",
            "kulmule",
        ],
        "Halibut": [
            "hälleflundra",
            "helleflynder",
            "kveite",
        ],
        "Ham": [
            "skinka",
            "skinke",
        ],
        # "Herring": [
        #    "Sild",
        #    "Sill",
        # ],
        "Kabanos": [
            "kabanos",
            "cabanos",
        ],
        "Lamb": [
            "lam",
            "lamm",
        ],
        # "Lemon Sole": [  # remove
        #    "Rødtunge",
        #    "Lomre",
        #    "Citrontunga",
        # ],
        "Lentils": [
            "linser",
        ],
        "Lobster": [
            "hummer",
        ],
        "Lutefisk": [
            "lutfisk",
            "tørfisk",
            "lutefisk",
        ],
        "Mackerel": [
            "makrel",
            "makrell",
            "makrill",
        ],
        "Minced Meat": [
            "kjøttdeig",
            "hakket kød",
            "köttfärs",
        ],
        "Mixed Ground Meat": [
            "blandfärs",
            "blandet fars",
            "blandet kjøttdeig",
        ],
        "Monkfish": [
            "havtaske",
            "breiflabb",
            "marulk",
        ],
        "Moose": [
            "älg",
            "elg",
        ],
        "Mortadella": [
            "mortadella",
        ],
        "Mussels": [
            "blåskjell",
            "muslinger",
            "musslor",
        ],
        "Omph": ["omph", "oumph"],
        "Ox": [
            "okse",
            "oxe",
        ],
        # "Oysters": [  # remove
        #    "Østers",
        #    "Ostron",
        # ],
        "Pastrami": [
            "pastrami",
        ],
        "Perch": [
            "abbor",
            "abborre",
            "aborre",
        ],
        "Pike": [
            "gädda",
            "gedde",
            "gjedde",
        ],
        # "Plaice": [
        #    "Rødspette",
        #    "Spätta",
        #    "Rødspætte",
        # ],
        "Pollock": [
            "sei",
            "sej",
            "pollock",
        ],
        "Pork": [
            "svinekjøtt",
            "svinekød",
            "fläsk",
        ],
        "Pork Chop": [
            "fläskkotlett",
            "kotelet",
            "svinekotelett",
        ],
        "Pork Shoulder": [
            "fläskaxel",
            "bogstek",
            "svineskulder",
        ],
        "Pork Tenderloin": [
            "svinemørbrad",
            "svinefilet",
            "fläskfilé",
        ],
        "Quorn": [
            "quorn",
        ],
        "Rabbit": [
            "kanin",
        ],
        "Reindeer": [
            "reinsdyr",
            "ren",
            "rensdyr",
        ],
        "Ribeye Steak": [
            "entrecote",
            "ribeye",
            "ribeye biff",
        ],
        "Roast": [
            "stek",
            "steg",
            "stek",
        ],
        "Roast Beef": [
            "roastbiff",
            "rostbiff",
            "roastbeef",
        ],
        # "Roe": [  # remove
        #    "Rogn",
        #    "Rom",
        # ],
        "Roe Deer": [
            "rådjur",
            "rådyr",
        ],
        "Salami": [
            "salami",
        ],
        "Salmon": [
            "laks",
            "lax",
        ],
        "Salsiccia": [
            "salsiccia",
        ],
        "Sardine": [
            "sardin",
        ],
        # "Scallops": [
        #    "Kammuslinger",
        #    "Kamskjell",
        #    "Pilgrimsmusslor",
        # ],
        "Scampi": [
            "scampi",
        ],
        "Shrimp": [
            "räka",
            "reje",
            "reke",
        ],
        "Sirloin Steak": [
            "ryggbiff",
            "tykstegsbøf",
            "mørbrad",
        ],
        "Skrei": [
            "skrei",
        ],
        "Smoked Pork Loin": [
            "røget Svinekam",
            "røkt Svinekam",
            "rökt Sidfläsk",
        ],
        "Spare Ribs": [
            "spareribs",
            "revbensspjäll",
        ],
        "Squid": [
            "blæksprutte",
            "bläckfisk",
            "blekksprut",
        ],
        "Tofu": [
            "tofu",
        ],
        "Tuna": [
            "tunfisk",
            "tonfisk",
            "tun",
        ],
        "Turbot": [
            "piggvar",
            "pighvar",
        ],
        "Turkey": [
            "kalkun",
            "kalkon",
        ],
        "Veal": [
            "kalvkött",
            "kalvekjøtt",
            "kalvekød",
        ],
        # "Vendace Roe": [  # remove
        #    "Löjrom",
        #    "Løyrom",
        #    "Løjrom",
        # ],
        "Vienna Sausage": [
            "wienkorv",
            "wienerpølse",
        ],
        # "Whitefish": [  # remove
        #    "Hvidfisk",
        #    "Sik",
        # ],
        "Wild Boar": [
            "villsvin",
            "vildsvin",
        ],
        "Zander": [
            "gjørs",
            "gös",
            "sandart",
        ],
    },
}


cuisine_mapping: dict[str, dict[int, list[str]]] = {
    "African": {
        1: [
            "afrikansk",
            "etiopisk",
            "tunisiske",
            "nordafrikansk",
            "vestafrikansk",
            "sør-afrikansk",
            "nigeriansk",
            "ghanesisk",
            "senegalesisk",
            "afrika",
        ],
        5: [
            "afrikansk",
            "etiopisk",
            "tunisiskt",
            "nordafrikansk",
            "västafrikansk",
            "sydafrikansk",
            "nigeriansk",
            "ghanesisk",
            "senegalesisk",
            "afrika",
        ],
        6: [
            "afrikansk",
            "etiopisk",
            "tunesiske",
            "nordafrikansk",
            "vestafrikansk",
            "sydafrikansk",
            "nigeriansk",
            "ghanesisk",
            "senegalesisk",
            "afrika",
        ],
    },
    "American": {
        1: [
            "amerikansk",
            "amerika",
            "usa",
            "american",
        ],
        5: [
            "amerikansk",
            "amerika",
            "usa",
            "american",
        ],
        6: [
            "amerikansk",
            "amerika",
            "usa",
            "american",
        ],
    },
    "Arabian": {
        1: [
            "arabisk",
            "levantinsk",
            "saudiarabisk",
            "jordansk",
            "palestinsk",
            # "syrisk",
            "emiratisk",
            "kuwaitisk",
            "irakisk",
            "marokkansk",
        ],
        5: [
            "arabisk",
            "levantinsk",
            "saudiarabisk",
            "jordansk",
            "palestinsk",
            # "syrisk",
            "emiratisk",
            "kuwaitisk",
            "irakisk",
            "oman",
            "marockansk",
        ],
        6: [
            "arabisk",
            "levantinsk",
            "saudiarabisk",
            "jordansk",
            "palestinsk",
            # "syrisk",
            "emiratisk",
            "kuwaitisk",
            "irakisk",
            "oman",
            "marokkansk",
        ],
    },
    "Asian": {
        1: [
            "asiatisk",
            "asia",
            "orientalsk",
            "østlig",
        ],
        5: [
            "asiatisk",
            "asien",
            "orientalisk",
            "östlig",
        ],
        6: [
            "asiatisk",
            "asien",
            "orientalsk",
            "østlig",
        ],
    },
    "British": {
        1: [
            "britisk",
            "engelsk",
        ],
        5: [
            "brittisk",
            "engelsk",
        ],
        6: [
            "britisk",
            "engelsk",
        ],
    },
    "Chinese": {
        1: [
            "kinesisk",
            "kina",
            "sichuan",
            "kantonesisk",
        ],
        5: [
            "kinesisk",
            "kina",
            "sichuan",
            "kantonesisk",
        ],
        6: [
            "kinesisk",
            "kina",
            "sichuan",
            "kantonesisk",
        ],
    },
    "Danish": {
        1: [
            "dansk",
            "danmark",
        ],
        5: [
            "dansk",
            "danmark",
        ],
        6: [
            "dansk",
            "danmark",
            "husmandskost",
            "nordisk",
        ],
    },
    # "Finnish": {
    #     1: [
    #         "finsk",
    #         "finland",
    #     ],
    #     5: [
    #         "finsk",
    #         "finland",
    #     ],
    #     6: [
    #         "finsk",
    #         "finland",
    #     ],
    # },
    "French": {
        1: [
            "fransk",
            "frankrike",
        ],
        5: [
            "fransk",
            "frankrike",
        ],
        6: [
            "fransk",
            "frankrig",
        ],
    },
    "Fusion Food": {
        1: [
            "fusjonsmat",
            "fusion",
            "fusion-food",
        ],
        5: [
            "fusion",
            "fusion-food",
        ],
        6: [
            "fusion",
            "fusion-food",
            "fusionsmad",
        ],
    },
    "Greek": {
        1: [
            "gresk",
            "hellas",
        ],
        5: [
            "grekisk",
            "grekland",
        ],
        6: [
            "græsk",
            "grækenland",
        ],
    },
    "Hawaiian": {
        1: [
            "hawaiisk",
            "hawaii",
        ],
        5: [
            "hawaiiansk",
            "hawaii",
        ],
        6: [
            "hawaiiansk",
            "hawaii",
        ],
    },
    "Indian": {
        1: [
            "indisk",
            "india",
        ],
        5: [
            "indisk",
            "indien",
        ],
        6: [
            "indisk",
            "indien",
        ],
    },
    "Italian": {
        1: [
            "italiensk",
            "italia",
        ],
        5: [
            "italiensk",
            "italien",
        ],
        6: [
            "italiensk",
            "italien",
        ],
    },
    "Japanese": {
        1: [
            "japansk",
            "japan",
        ],
        5: [
            "japansk",
            "japan",
        ],
        6: [
            "japansk",
            "japan",
        ],
    },
    "Korean": {
        1: [
            "koreansk",
            "korea",
        ],
        5: [
            "koreansk",
            "korea",
        ],
        6: [
            "koreansk",
            "korea",
        ],
    },
    "Lebanese": {
        1: [
            "libanesisk",
            "libanon",
        ],
        5: [
            "libanesisk",
            "libanon",
        ],
        6: [],
    },
    "Mediterranean": {
        1: [
            "middelhavet",
            "middelhavs",
        ],
        5: [
            "medelhavsmat",
            "medelhavet",
        ],
        6: [
            "middelhavsmad",
            "middelhavet",
        ],
    },
    "Mexican": {
        1: [
            "meksikansk",
            "mexico",
            "latinamerika",
        ],
        5: [
            "mexikansk",
            "mexiko",
            "mexikanskt",
            "latinamerika",
        ],
        6: [
            "mexicansk",
            "mexico",
            "latinamerika",
        ],
    },
    "Middle Eastern": {
        1: [
            "midtøsten",
            "tyrkisk",
        ],
        5: [
            "mellanöstern",
            "turkisk",
        ],
        6: [
            "mellemøstlig",
            "mellemøsten",
            "tyrkisk",
        ],
    },
    "Norwegian": {
        1: [
            "norsk",
            "norge",
            "husmannskost",
            "nordisk",
        ],
        5: [
            "norsk",
            "norge",
        ],
        6: [
            "norsk",
            "norge",
        ],
    },
    "Persian": {
        1: [
            "persisk",
            "iran",
        ],
        5: [
            "persisk",
            "iran",
        ],
        6: [
            "persisk",
            "iran",
        ],
    },
    "South American": {
        1: [
            "søramerikansk",
            "sør-amerikansk",
            "søramerika",
            "sør-amerika",
            "brasil",
            "peru",
            "argentina",
            "latinamerika",
            "latin-amerika",
        ],
        5: [
            "sydamerikansk",
            "sydamerika",
            "brasilien",
            "peru",
            "argentina",
            "latinamerika",
            "latin-amerika",
        ],
        6: [
            "sydamerikansk",
            "sydamerika",
            "brasilien",
            "peru",
            "argentina",
            "latinamerika",
            "latin-amerika",
        ],
    },
    "Spanish": {
        1: [
            "spansk",
            "spania",
        ],
        5: [
            "spansk",
            "spanien",
        ],
        6: [
            "spansk",
            "spanien",
        ],
    },
    "Swedish": {
        1: [
            "svensk",
            "sverige",
        ],
        5: [
            "svensk",
            "sverige",
            "nordisk",
            "husmanskost",
        ],
        6: [
            "svensk",
            "sverige",
        ],
    },
    "Tex-mex": {
        1: [
            "tex-mex",
            "texmex",
            "tex mex",
            "texas",
        ],
        5: [
            "tex-mex",
            "texas",
        ],
        6: [
            "tex-mex",
            "tex mex",
            "texas",
        ],
    },
    "Thai": {
        1: [
            "thai",
            "thailand",
            "thailandsk",
        ],
        5: [
            "thailändsk",
            "thailand",
            "thai",
        ],
        6: [
            "thailandsk",
            "thailand",
            "thai",
        ],
    },
    "Vietnamese": {
        1: [
            "vietnamesisk",
            "vietnam",
        ],
        5: [
            "vietnamesisk",
            "vietnam",
        ],
        6: [
            "vietnamesisk",
            "vietnam",
        ],
    },
}


protein_penalty_pairs: set[tuple[str, str]] = {
    ("citron", "rødtunge"),
    ("citron", "citrontunga"),
    ("rødløg", "rødspette"),
    ("rødløg", "rødtunge"),
    ("persille", "sill"),
    ("bredbladet persille", "sill"),
    ("mel", "kummel"),
    ("melk", "kummel"),
    ("persillade", "sill"),
    ("æg", "kvæg"),
    ("kruspersille", "sill"),
    ("østershatte", "østers"),
    ("solsikkekerner", "sik"),
    ("græskar- & solsikkekerner", "sik"),
    ("pasta", "pastrami"),
    ("kalamataoliven", "lam"),
    ("kalamataoliver", "lam"),
    ("sitron", "ostron"),
    ("bladpersille", "sill"),
    ("aromasopp", "rom"),
    ("romanesco", "rom"),
    ("olja", "kolja"),
    ("romansallad", "rom"),
    ("ostronskivling", "ostron"),
    ("meksikanske fiskepinner", "sik"),
    ("chiliflak", "laks"),
    ("frisk", "lutfisk"),
    ("frisk", "tørfisk"),
    ("frisk", "lutefisk"),
    ("frisk", "bläckfisk"),
    ("frisk", "tunfisk"),
    ("frisk", "tonfisk"),
    ("grön sparris", "spareribs"),
    ("kyllingbryst u/skinn (saktevoksende)", "okse"),
    ("kycklingbuljong", "kyckling"),
}


dish_penalty_words: list[str] = ["steinsoppaioli", "gratine"]
