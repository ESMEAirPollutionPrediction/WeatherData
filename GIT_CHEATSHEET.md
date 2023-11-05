# Worflow GIT

## Commande GIT à utiliser

### `git status`

La commande `git status` est utilisée pour afficher l'état actuel de votre répertoire de travail. Elle vous montre les fichiers qui ont été modifiés, ceux qui sont en attente de validation (staging) et d'autres informations utiles sur votre branche Git.
***
### `git -am "commit message"`Only working in Ubuntu

La commande `git -am "commit message"` est une combinaison de deux actions :
- `git add .` : Elle ajoute tous les fichiers modifiés à la zone de staging.
- `git commit -m "commit message"` : Elle crée un commit avec les fichiers ajoutés à la zone de staging et le message de commit que vous avez spécifié entre guillemets.

Cela permet de rapidement ajouter et valider des modifications en une seule commande.
***
### `git branch branch_name`

La commande `git branch branch_name` est utilisée pour créer une nouvelle branche avec le nom spécifié (`branch_name` dans cet exemple).
***
### `git checkout branch_name`

La commande `git checkout branch_name` est utilisée pour changer de branche. Elle vous permet de basculer vers une branche différente existante dans votre dépôt Git.
***
### `git checkout -b branch_name`

La commande `git checkout -b branch_name` combine deux actions :
- `git branch branch_name` : Elle crée une nouvelle branche avec le nom spécifié.
- `git checkout branch_name` : Elle bascule sur la nouvelle branche créée.

Cela est couramment utilisé pour créer et se déplacer vers une nouvelle branche en une seule commande.
***
### `git branch`

La commande `git branch` permet d'afficher la liste de toutes les branches dans votre dépôt Git. La branche actuelle sera mise en évidence.
***
### `git merge branch_name`

La commande `git merge branch_name` est utilisée pour fusionner les modifications de la branche spécifiée (`branch_name`) dans la branche actuelle. Cela combine les changements des deux branches.
***
### `git push`

La commande `git push` est utilisée pour pousser vos commits locaux vers un dépôt distant. Cela met à jour le dépôt distant avec vos modifications locales.
***
### `git pull`

La commande `git pull` est utilisée pour récupérer les dernières modifications depuis un dépôt distant et les fusionner automatiquement dans votre branche locale. Elle équivaut à exécuter `git fetch` suivi de `git merge`.
***

### `git tag`

La commande `git tag` est utilisée pour créer, lister ou supprimer des tags dans un dépôt Git. Les tags sont généralement utilisés pour marquer des points spécifiques de l'historique des commits, comme des versions logicielles stables. Vous pouvez créer des tags légers (tags simples sans métadonnées) ou annotés (tags avec des informations supplémentaires, comme un message).

- Pour créer un tag léger : `git tag tag_name`
- Pour créer un tag annoté : `git tag -a tag_name -m "message"`
***
### `git stash`

La commande `git stash` est utilisée pour temporairement stocker des modifications locales non validées (dans la zone de staging) afin que vous puissiez passer à une autre tâche ou branche sans valider ces modifications.

- Pour stocker vos modifications : `git stash save "description"`
- Après avoir stocké vos modifications, vous pouvez passer à une autre branche ou tâche.
- Pour appliquer vos modifications stockées, vous pouvez utiliser `git stash apply` suivi du nom de la stash (ou le dernier stash est utilisé si aucun nom n'est spécifié).
***
### `git stash list`

La commande `git stash list` permet de lister toutes les stashes actuelles dans votre dépôt. Elle affiche les informations sur chaque stash, telles que son nom et la description associée.
***

### `git stash pop`

La commande `git stash pop` est utilisée pour appliquer la dernière stash stockée et la supprimer de la liste des stashes. Cela vous permet de restaurer vos modifications temporaires et de les appliquer à votre branche de travail actuelle.

Ces commandes `git stash`, `git stash list`, et `git stash pop` sont particulièrement utiles lorsque vous travaillez sur une tâche, mais devez rapidement passer à autre chose sans commettre ou abandonner vos modifications en cours.


## Git flow

A décider

Proposition 1: GitHub central : main, feature, bugFix

Proposition 2: Git local copy Github

## Repétabilité

Pour mettre à jour requirements.txt à partir des librairies installées sur votre environnement on utilse cette commande :
```bash
pip freeze > requirements.txt
```
Pour installer les librairies présentes sur requirements.txt sur votre environnement on utilse cette commande :
```bash
pip install -r requirements.txt
```
