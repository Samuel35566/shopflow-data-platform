-- TABLE DE FAITS des ventes.
-- GRAIN : une ligne = un article d'une commande. Les montants sont en euros.
-- Choix métier :
--   * les lignes des commandes en QUARANTAINE sont exclues (inner join avec stg_commandes) ;
--   * les commandes annulées / remboursées / non payées sont GARDÉES mais SIGNALÉES ;
--   * le client est rattaché à la VERSION valable au moment de la commande (SCD2).

with lignes as (
    select * from {{ ref('stg_lignes_commande') }}
),

commandes as (
    select * from {{ ref('stg_commandes') }}
),

-- UNE ligne par commande payée (le distinct évite de dupliquer les ventes)
paiements_reussis as (
    select distinct commande_id
    from {{ ref('stg_paiements') }}
    where statut = 'succeeded'
),

produits  as (select produit_id  from {{ ref('dim_produit') }}),
clients   as (select client_sk, client_id, valide_du, valide_au from {{ ref('dim_client') }}),
entrepots as (select entrepot_id from {{ ref('dim_entrepot') }})

select
    -- identifiants de la ligne
    l.commande_id,
    l.numero_ligne,

    -- clés vers les dimensions (membre « inconnu » si la valeur n'y est pas)
    c.date_commande,
    coalesce(cl.client_sk, '-1')        as client_sk,
    coalesce(cl.client_id, -1)          as client_id,
    coalesce(p.produit_id, -1)          as produit_id,
    coalesce(e.entrepot_id, 'INCONNU')  as entrepot_id,
    l.produit_id                        as produit_id_source,

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
inner join commandes c           on l.commande_id = c.commande_id
-- SCD2 : le bon client ET la version valable à la date de la commande
left join  clients cl            on  c.client_id   = cl.client_id
                                 and c.commande_le >= cl.valide_du
                                 and c.commande_le <  cl.valide_au
left join  produits p            on l.produit_id  = p.produit_id
left join  entrepots e           on c.entrepot    = e.entrepot_id
left join  paiements_reussis pr  on l.commande_id = pr.commande_id
