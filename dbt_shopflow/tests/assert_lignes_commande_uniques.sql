-- Test SINGULIER : un couple (commande_id, numero_ligne) ne doit apparaître qu'une fois.
-- dbt considère le test RÉUSSI si cette requête ne renvoie AUCUNE ligne.
select
    commande_id,
    numero_ligne,
    count(*) as nb
from {{ ref('stg_lignes_commande') }}
group by commande_id, numero_ligne
having count(*) > 1
