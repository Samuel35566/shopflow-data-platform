-- DIMENSION CLIENT historisée (SCD type 2) : une ligne par VERSION d'un client.
-- client_sk  = clé technique d'UNE version (c'est elle que la table de faits utilise)
-- client_id  = identifiant métier du client (le même pour toutes ses versions)

with versions as (
    select * from {{ ref('snap_clients') }}
)

select
    dbt_scd_id                          as client_sk,
    client_id,
    prenom,
    nom,
    email,
    email_valide,
    coalesce(pays, 'Non renseigné')     as pays,
    coalesce(segment, 'Non renseigné')  as segment,
    date_inscription,
    -- La 1re version d'un client est valable « depuis toujours » : sinon, les commandes
    -- passées AVANT le premier snapshot ne trouveraient aucune version.
    case
        when row_number() over (partition by client_id order by dbt_valid_from) = 1
            then timestamp '1900-01-01'
        else cast(dbt_valid_from as timestamp)
    end                                                          as valide_du,
    coalesce(cast(dbt_valid_to as timestamp), timestamp '9999-12-31')  as valide_au,
    dbt_valid_to is null                                         as est_version_actuelle
from versions

union all

select '-1', -1, null, 'Client inconnu', null, false, 'Non renseigné', 'Non renseigné', null,
       timestamp '1900-01-01', timestamp '9999-12-31', true
