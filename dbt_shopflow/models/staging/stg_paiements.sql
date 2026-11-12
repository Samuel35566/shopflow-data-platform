-- STAGING des paiements (montants déjà convertis en euros par Spark).

with source as (
    select * from {{ source('silver', 'payments') }}
)

select
    payment_id        as paiement_id,
    order_id          as commande_id,
    status            as statut,
    currency          as devise,
    montant_devise,
    taux_pour_1_eur,
    montant_eur,
    paye_le
from source
