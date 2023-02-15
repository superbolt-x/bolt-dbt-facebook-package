{%- macro conversion_alias_config(conversion_name) -%}

{%- set config = (
    
    {"original_name": 'link_click', "alias": 'link_clicks'},
    {"original_name": 'purchase', "alias": 'web_purchases'},
    {"original_name": 'purchase_value', "alias": 'web_revenue'},
    {"original_name": 'offsite_conversion.fb_pixel_purchase', "alias": 'purchases'},
    {"original_name": 'offsite_conversion.fb_pixel_purchase_value', "alias": 'revenue'},
    {"original_name": 'add_to_cart', "alias": 'web_add_to_cart'},
    {"original_name": 'add_to_cart_value', "alias": 'web_add_to_cart_value'},
    {"original_name": 'offsite_conversion.fb_pixel_add_to_cart', "alias": 'add_to_cart'},
    {"original_name": 'offsite_conversion.fb_pixel_add_to_cart_value', "alias": 'add_to_cart_value'},
    {"original_name": 'initiate_checkout', "alias": 'web_initiate_checkout'},
    {"original_name": 'offsite_conversion.fb_pixel_initiate_checkout', "alias": 'initiate_checkout'},
    {"original_name": 'onsite_conversion.lead_grouped', "alias": 'onfacebook_leads'},
    {"original_name": 'onsite_conversion.post_save', "alias": 'post_save'},
    {"original_name": 'offsite_conversion.fb_pixel_lead', "alias": 'website_leads'}

    )-%}

{{ return (config | selectattr('original_name', 'equalto', conversion_name)| map(attribute='alias')|join(' ')) }}


{%- endmacro -%}
