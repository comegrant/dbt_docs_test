# Serves as an execution script for the model
# Should hold the code needed for execution in production
# Below is an example of the expected format.

# import prep, model, config
from gen import Gen
import config

# from .config import PREP_CONFIG


prep = Gen(config.PREP_CONFIG).main()

"""
train_data, test_data = prep.Prep(PREP_CONFIG).main()  # Can also be class

m = model.Model(config)
m.fit(train_data)

output = m.predict(test_data)
"""
