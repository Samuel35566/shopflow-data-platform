-- Test SINGULIER de RÉCONCILIATION : le total du rapport doit être EXACTEMENT
-- le CA réel de la table de faits. Si une jointure perdait des ventes, ce test échouerait.
with rapport as (
    select sum(ca_reel) as total from {{ ref('rpt_ca_mensuel_categorie') }}
),
faits as (
    select sum(montant_ligne) as total from {{ ref('fct_ventes') }}
    where est_vente_valide and est_payee
)
select rapport.total as total_rapport, faits.total as total_faits
from rapport, faits
where rapport.total <> faits.total
