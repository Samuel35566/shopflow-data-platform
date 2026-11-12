-- STAGING des commandes : une vue propre sur la table silver.
-- Règle du staging : 1 modèle = 1 source, on renomme et on type, AUCUNE logique métier.

with source as (
    select * from {{ source('silver', 'orders') }}
)

select
    order_id                  as commande_id,
    customer_id               as client_id,
    order_date                as commande_le,     -- date ET heure
    cast(order_date as date)  as date_commande,   -- juste le jour (servira pour dim_date)
    status                    as statut,
    total_amount              as montant_total,
    currency                  as devise,
    warehouse                 as entrepot,
    updated_at                as modifie_le
from source