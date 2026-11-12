-- TABLE DE FAITS des ventes.
-- GRAIN : une ligne = un article d'une commande. Les montants sont en euros.
-- Choix métier :
--   * les lignes des commandes en QUARANTAINE sont exclues (inner join avec stg_commandes) ;
--   * les commandes annulées / remboursées / non payées sont GARDÉES mais SIGNALÉES :
--     chaque indicateur décide s'il les compte (colonnes est_vente_valide et est_payee).

with lignes as (
    select * from {{ ref('stg_lignes_commande') }}
),

commandes as (
    select * from {{ ref('stg_commandes') }}
),

-- UNE ligne par commande payée. Le « distinct » est vital : si une commande avait
-- deux paiements réussis, la jointure doublerait ses lignes de vente (et son montant !).
paiements_reussis as (
    select distinct commande_id
    from {{ ref('stg_paiements') }}
    where statut = 'succeeded'
),

produits  as (select produit_id  from {{ ref('dim_produit') }}),
clients   as (select client_id   from {{ ref('dim_client') }}),
entrepots as (select entrepot_id from {{ ref('dim_entrepot') }})

select
    -- identifiants de la ligne
    l.commande_id,
    l.numero_ligne,

    -- clés vers les dimensions (membre « inconnu » si la valeur n'y est pas)
    c.date_commande,
    coalesce(cl.client_id, -1)          as client_id,
    coalesce(p.produit_id, -1)          as produit_id,
    coalesce(e.entrepot_id, 'INCONNU')  as entrepot_id,
    l.produit_id                        as produit_id_source,   -- l'identifiant d'origine, pour enquêter

    -- indicateurs
    c.statut                                     as statut_commande,
    c.statut not in ('cancelled', 'refunded')    as est_vente_valide,
    pr.commande_id is not null                   as est_payee,

    -- mesures
    l.quantite,
    l.prix_unitaire,
    l.remise,
    l.montant_ligne

from lignes l
inner join commandes c           on l.commande_id = c.commande_id   -- exclut les commandes en quarantaine
left join  clients cl            on c.client_id   = cl.client_id
left join  produits p            on l.produit_id  = p.produit_id
left join  entrepots e           on c.entrepot    = e.entrepot_id
left join  paiements_reussis pr  on l.commande_id = pr.commande_id
