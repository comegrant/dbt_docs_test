# Recipe Tagging

A Python project by Cheffelo.

## Project overview

Recipe Tagging is an automated solution for classifying recipes into various SEO-optimized categories. This system analyzes recipe content and automatically assigns relevant taxonomies before sending the results to azure.

The project automatically tags recipes with the following taxonomies:
- **Cuisines**: Identifies the culinary origin of recipes using machine learning (e.g., Italian, Mexican, Thai)
- **Preferences**: Tags recipes with diet-related preferences (e.g., low-calorie, dairy-free)
- **Categories**: Classifies recipes by main ingredient (e.g., meat, seafood)
- **Proteins**: Identifies protein sources in recipes (e.g., chicken, filet, tofu)
- **Dish Types**: Categorizes by meal type (e.g., burger, pizza)
- **Traits**: Tags special characteristics (e.g., family friendly, quick to prepare)

NB! Family friendly relies on a taxonomy. 


## How it works

- Data Retrieval: Fetches recipe data from the database for a specific language
- Taxonomy Extraction: Processes recipes to identify relevant tags across six taxonomy types
    - Rule-based matching for preferences, categories, proteins, traits, and dishes
    - Machine learning prediction for cuisine classification using trained SVC
- Output Generation: Creates a structured output with taxonomy IDs for each recipe
- Data Storage: Saves results to Azure storage for integration with RecipeHub

### Streamlit app

Run the streamlit app locally:

````
chef up app
```