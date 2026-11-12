-- Test SINGULIER : le grain de fct_ventes est respecté (pas de fan-out).
-- Un couple (commande_id, numero_ligne) ne doit apparaître qu'UNE fois.
select commande_id, numero_ligne, count(*) as nb
from {{ ref('fct_ventes') }}
group by commande_id, numero_ligne
having count(*) > 1
