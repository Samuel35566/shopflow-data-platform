-- Test SINGULIER : chaque client a exactement UNE version actuelle.
-- Deux versions « actuelles » = historique corrompu.
select client_id, count(*) as nb_versions_actuelles
from {{ ref('dim_client') }}
where est_version_actuelle
group by client_id
having count(*) <> 1
