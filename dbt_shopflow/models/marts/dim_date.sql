-- DIMENSION DATE : un jour par ligne, sur les années entières couvertes par les commandes.
-- Elle est GÉNÉRÉE : aucun système source ne contient « la liste des jours ».

with bornes as (
    select
        cast(date_trunc('year', min(date_commande)) as timestamp)                              as debut,
        cast(date_trunc('year', max(date_commande)) as timestamp) + interval 1 year - interval 1 day as fin
    from {{ ref('stg_commandes') }}
),

jours as (
    select cast(unnest(generate_series(debut, fin, interval 1 day)) as date) as date_jour
    from bornes
)

select
    date_jour,
    year(date_jour)     as annee,
    quarter(date_jour)  as trimestre,
    month(date_jour)    as mois,
    ['janvier', 'février', 'mars', 'avril', 'mai', 'juin', 'juillet',
     'août', 'septembre', 'octobre', 'novembre', 'décembre'][month(date_jour)]  as nom_mois,
    isodow(date_jour)   as jour_semaine,   -- 1 = lundi … 7 = dimanche
    ['lundi', 'mardi', 'mercredi', 'jeudi', 'vendredi', 'samedi', 'dimanche'][isodow(date_jour)]  as nom_jour,
    isodow(date_jour) in (6, 7)  as est_week_end
from jours
