-- DIMENSION CLIENT : un client par ligne,
-- + un membre « inconnu » (client_id = -1) pour les commandes de clients absents du CRM.

with clients as (
    select * from {{ ref('stg_clients') }}
)

select
    client_id,
    prenom,
    nom,
    email,
    email_valide,
    coalesce(pays, 'Non renseigné')     as pays,
    coalesce(segment, 'Non renseigné')  as segment,
    date_inscription
from clients

union all

select -1, null, 'Client inconnu', null, false, 'Non renseigné', 'Non renseigné', null
