# Product Concepts
{% docs column__product_concept_id %}

The primary key of the product concepts in the product layer database. Product concepts is a grouping of products, almost like the product types, but on a more general level. 

{% enddocs %}

{% docs column__product_concept_name %}

The name the product concepts in the product layer database. Product concepts is a grouping of products, almost like the product types, but on a more general level. 


{% enddocs %}

{% docs column__product_concept_description %}

The description of the product concepts in the product layer database. Product concepts is a grouping of products, almost like the product types, but on a more general level. 

{% enddocs %}

# Product Types
{% docs column__product_type_id %}

The primary key of the product types in the product layer database.

{% enddocs %}

{% docs column__product_type_name %}

The name of the product types in the product layer database.

{% enddocs %}

{% docs column__product_type_description %}

The description of the product types in the product layer database.

{% enddocs %}

{% docs column__is_physical_product %}

True if the product is a physical product (like Mealboxes). 
False if the product is not a physical product (like Administrative products or Giftcards).

{% enddocs %}

# Products
{% docs column__product_id %}

The primary key of the products in the product layer database.

{% enddocs %}

{% docs column__product_name %}

The name of the products in the product layer database.

{% enddocs %}

{% docs column__product_description %}

The description of the products in the product layer database.

{% enddocs %}

# Product Statuses

{% docs column__product_status_id %}

The primary key of the product statuses in the product layer database. 

{% enddocs %}

{% docs column__product_status_name %}

The name of the product statuses in the product layer database. 

{% enddocs %}

# Product Variations
{% docs column__product_variation_id %}

The unique identifier of the product variations in the product layer database. The product variation is the lowest granularity of a product and represents different variations of a product in terms of meals and portions for instance. The variation is what the customer order when they make a purchase.

{% enddocs %}

{% docs column__sku %}

The stock keeping unit (sku) of the product variations.

{% enddocs %}

# Product Variations Companies

{% docs column__product_variation_name %}

The name of each variation name in each company. A product variation has a specified size, meals and portions, unlike the corresponding product where the size, portions and meals is not specified. 

{% enddocs %}

{% docs column__product_variation_description %}

Description of a product variation. 

{% enddocs %}

{% docs column__sent_to_frontend %}

True if the product variation will be visible on the webpage within the next 4 wekks else false. When exactly it will be shown on the front page is determined by rules on the campaigns set up in the platform_campaign_product-table. The field is used by the grocery team to validate if the set up of groceries is correct.

{% enddocs %}

# Product Variation Attribute Values

{% docs column__attribute_value %}

The value of a selected attribute for a product variation. 
E.g. if the selected attribute is "Meals" and the product variation is "Vegetar 3 Middager 4 Personer", then the attribute value will be 3, since the variation has 3 meals. 

{% enddocs %}

# Product Variation Attribute Templates
{% docs column__attribute_id %}

The id of the attribute connected to a product variation. 

{% enddocs %}

{% docs column__attribute_name %}

The name of the attribute connected to a product variation. For example "Meals" or "Portions". 

{% enddocs %}

{% docs column__attribute_default_value %}

The default value of a product variation attribute, which should be used if no other attribute is specified.

{% enddocs %}

{% docs column__attribute_data_type %}

Gives information about the data type of the attribute value, e.g. if it is a String, Decimal, Boolean etc. 

{% enddocs %}
