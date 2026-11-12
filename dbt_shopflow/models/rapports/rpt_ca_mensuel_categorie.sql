-- RAPPORT 1 : chiffre d'affaires RÉEL par mois et par catégorie.
-- RÉEL = commande ni annulée ni remboursée, ET paiement réussi.

with ventes as (
    select * from {{ ref('fct_ventes') }}
    where est_vente_valide and est_payee
)

select
    d.annee,
    d.mois,
    d.nom_mois,
    p.categorie,
    count(distinct v.commande_id)  as nb_commandes,
    sum(v.quantite)                as articles_vendus,
    sum(v.montant_ligne)           as ca_reel
from ventes v
join {{ ref('dim_date') }}    d on v.date_commande = d.date_jour
join {{ ref('dim_produit') }} p on v.produit_id    = p.produit_id
group by all
order by d.annee, d.mois, ca_reel desc
