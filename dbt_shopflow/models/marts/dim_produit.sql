-- DIMENSION PRODUIT : un produit par ligne,
-- + un membre « inconnu » (produit_id = -1) pour les ventes de produits hors catalogue.

with produits as (
    select * from {{ ref('stg_produits') }}
)

select
    produit_id,
    nom,
    coalesce(marque, 'Non renseignée')  as marque,
    categorie,
    prix                                as prix_catalogue
from produits

union all

select -1, 'Produit inconnu', 'Non renseignée', 'Inconnue', null
