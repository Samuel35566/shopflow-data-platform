-- STAGING des clients.
-- Le téléphone n'est volontairement PAS repris : on ne fait remonter vers l'analyse
-- que les données personnelles dont on a besoin (principe de minimisation, RGPD).

with source as (
    select * from {{ source('silver', 'clients') }}
)

select
    client_id,
    prenom,
    nom,
    email,
    email_valide,
    pays,
    segment,
    date_inscription
from source
