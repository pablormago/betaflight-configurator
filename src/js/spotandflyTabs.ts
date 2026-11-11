/*
 * This file is part of Betaflight.
 *
 * Betaflight is free software. You can redistribute this software
 * and/or modify this software under the terms of the GNU General
 * Public License as published by the Free Software Foundation,
 * either version 3 of the License, or (at your option) any later
 * version.
 *
 * Betaflight is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *
 * See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public
 * License along with this software.
 *
 * If not, see <http://www.gnu.org/licenses/>.
 */

/**
 * Spot&Fly: esta web solo sirve para configurar por la sesión remota el dron que se comparte desde la app Spot&Fly,
 * así que enseña las mismas pestañas que la app (Pablo, 11/11/2026). Fuera: CLI (en remoto no se delega), sensores,
 * preajustes, correcciones, servos, LED, plan de vuelo, registros, caja negra, auto-ajuste, firmware, ayuda y
 * pre-vuelo. Filtros y Rates de la app están dentro de «pid_tuning».
 */
export const SPOTANDFLY_TABS: ReadonlySet<string> = new Set([
    "landing",
    "setup",
    "ports",
    "configuration",
    "power",
    "failsafe",
    "pid_tuning",
    "receiver",
    "auxiliary",
    "gps",
    "motors",
    "osd",
    "vtx",
]);
