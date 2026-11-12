-- RAPPORT 3 : pour chaque produit, combien de VISITES HUMAINES de sa page, et combien de commandes ?
-- Les robots sont EXCLUS : environ 40 % du trafic, qui fausserait complètement le calcul.

with vues as (
    select produit_id, count(*) as vues_humaines
    from {{ ref('stg_visites') }}
    where type_page = 'produit'
      and not est_robot
      and code_http = 200           -- la page s'est bien affichée
      and produit_id is not null
    group by produit_id
),

ventes as (
    select produit_id, count(distinct commande_id) as commandes
    from {{ ref('fct_ventes') }}
    where est_vente_valide and est_payee
    group by produit_id
)

select
    p.produit_id,
    p.nom,
    p.categorie,
    coalesce(vu.vues_humaines, 0)                              as vues_humaines,
    coalesce(ve.commandes, 0)                                  as commandes,
    round(ve.commandes / nullif(vu.vues_humaines, 0), 2)       as commandes_par_vue
from {{ ref('dim_produit') }} p
left join vues   vu using (produit_id)
left join ventes ve using (produit_id)
where p.produit_id <> -1
order by commandes_par_vue desc nulls last
