#!/usr/bin/env bash
set -e

# Script de réinitialisation complète de l'environnement local
# Référence : Section 5.5 du cahier des charges

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="${SCRIPT_DIR}/../../.."

echo "⚠️  ============================================================== ⚠️"
echo "   ATTENTION : RÉINITIALISATION COMPLÈTE DE L'ENVIRONNEMENT LOCAL   "
echo "⚠️  ============================================================== ⚠️"
echo "Cette action va :"
echo " 1. Arrêter tous les conteneurs Docker (Kafka, Schema Registry, PostgreSQL)."
echo " 2. SUPPRIMER DÉFINITIVEMENT tous les volumes de données (Kafka logs, bases SQL)."
echo " 3. Supprimer tous les messages et topics existants."
echo ""

if [ "$1" != "--force" ]; then
    read -p "Êtes-vous certain de vouloir continuer ? Tapez 'OUI' pour confirmer : " CONFIRM
    if [ "$CONFIRM" != "OUI" ]; then
        echo "❌ Réinitialisation annulée par l'utilisateur."
        exit 0
    fi
fi

echo "🛑 Arrêt des conteneurs et suppression des volumes..."
cd "$ROOT_DIR"
docker compose down -v --remove-orphans 2>/dev/null || sudo docker compose down -v --remove-orphans

echo "🧹 Nettoyage des répertoires temporaires locaux si existants..."
rm -rf /tmp/kafka-logs /tmp/zookeeper 2>/dev/null || true

echo "✅ Environnement local entièrement réinitialisé à l'état vierge."
echo "Pour redémarrer un cluster propre, lancez : ./run.sh"
