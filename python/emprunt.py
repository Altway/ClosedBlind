from pathlib import Path

# Prepare the contract text
contract_text = """
CONTRAT DE PRÊT FAMILIAL

Entre les soussignés :
1. Le Prêteur
   Nom : ________________________
   Prénom : _____________________
   Date de naissance : ___________
   Adresse : ____________________

2. L’Emprunteur
   Nom : ________________________
   Prénom : _____________________
   Date de naissance : ___________
   Adresse : ____________________

Préambule
Le présent contrat a pour objet l’octroi par le Prêteur à l’Emprunteur d’un prêt familial pour l’acquisition de 32 ETH dans le cadre de la création d’un nœud Ethereum.

Article 1 – Montant du prêt
Le Prêteur consent à l’Emprunteur un prêt d’un montant principal de 30 000 € (trente mille euros).

Article 2 – Durée et taux
2.1 Durée : 12 ans à compter de la date de signature du présent contrat.
2.2 Taux : 0,00 % nominal annuel.

Article 3 – Période initiale
Pendant les deux premières années, l’Emprunteur ne rembourse que les intérêts, soit 0 €.

Article 4 – Échéancier initial
À compter de la date d’amorçage (achat effectif des ETH), l’Emprunteur remboursera 250 € par mois jusqu’à extinction du capital.

Article 5 – Remboursement anticipé
L’Emprunteur peut rembourser tout ou partie du capital à tout moment, sans frais ni pénalité.

Article 6 – Exigibilité à la demande
Le Prêteur peut exiger le remboursement intégral du capital restant dû à tout moment, sans justification ni pénalité.

Article 7 – Modification de l’échéancier
Le Prêteur peut modifier le montant et la temporalité des échéances à tout moment par notification écrite.

Article 8 – Annulation
Avant la date d’amorçage, le prêt peut être annulé et restitué intégralement sans délai ni frais.

Article 9 – Modalités de paiement
Les remboursements seront effectués par virement bancaire sur le compte indiqué par le Prêteur.

Article 10 – Déclaration fiscale
Le contrat sera déclaré à l’administration fiscale comme prêt familial afin d’éviter toute requalification en donation déguisée.

Article 11 – Dispositions générales
11.1 Intégralité : ce contrat constitue l’intégralité de l’accord entre les Parties.
11.2 Nullité partielle : si une clause est nulle, les autres restent en vigueur.
11.3 Droit applicable : droit français. Tout litige sera porté devant le tribunal compétent du domicile du Prêteur.

Fait à ________________, le ________________.

Le Prêteur                                         L’Emprunteur
(Signature)                                        (Signature)
"""

# Save to file
file_path = Path("./contrat_pret_familial.txt")
file_path.write_text(contract_text)

file_path
