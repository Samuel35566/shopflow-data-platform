-- DIMENSION ENTREPÔT : vient du seed entrepots.csv,
-- + un membre « inconnu » pour les commandes sans entrepôt.

with entrepots as (
    select * from {{ ref('entrepots') }}
)

select entrepot_id, ville, region
from entrepots

union all

select 'INCONNU', 'Non renseignée', 'Non renseignée'
