-- RAPPORT 2 : les 10 produits qui rapportent le plus (CA réel),
-- avec leur rang dans leur catégorie et leur part du chiffre d'affaires total.

with ventes_par_produit as (
    select
        produit_id,
        sum(quantite)       as articles_vendus,
        sum(montant_ligne)  as ca_reel
    from {{ ref('fct_ventes') }}
    where est_vente_valide and est_payee
      and produit_id <> -1                  -- un produit inconnu n'a rien à faire dans un classement
    group by produit_id
)

select
    rank() over (order by v.ca_reel desc)                              as rang,
    p.produit_id,
    p.nom,
    p.marque,
    p.categorie,
    v.articles_vendus,
    v.ca_reel,
    rank() over (partition by p.categorie order by v.ca_reel desc)     as rang_dans_categorie,
    round(100 * v.ca_reel / sum(v.ca_reel) over (), 2)                 as part_du_ca_pct
from ventes_par_produit v
join {{ ref('dim_produit') }} p using (produit_id)
qualify rank() over (order by v.ca_reel desc) <= 10
order by rang
