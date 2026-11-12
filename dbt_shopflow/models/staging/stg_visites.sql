-- STAGING des logs web : une page vue par ligne.

with source as (
    select * from {{ source('silver', 'web_logs') }}
)

select
    ip,
    visite_le,
    cast(visite_le as date)  as date_visite,
    methode,
    url,
    code_http,
    type_page,
    product_id               as produit_id,
    est_robot,
    user_agent
from source
