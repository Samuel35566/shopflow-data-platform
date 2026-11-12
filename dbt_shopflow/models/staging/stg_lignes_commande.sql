-- STAGING des lignes de commande : un article d'une commande par ligne.

with source as (
    select * from {{ source('silver', 'order_items') }}
)

select
    order_id              as commande_id,
    line_number           as numero_ligne,
    product_id            as produit_id,
    quantity              as quantite,
    unit_price            as prix_unitaire,
    discount              as remise,
    montant_ligne,
    produit_au_catalogue
from source