-- STAGING du catalogue produits.

with source as (
    select * from {{ source('silver', 'products') }}
)

select
    product_id   as produit_id,
    nom,
    marque,
    categorie,
    prix,
    updated_at   as modifie_le
from source
