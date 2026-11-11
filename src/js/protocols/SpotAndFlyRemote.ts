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

/*
 * «Spot&Fly remoto»: configura un dron que otra persona comparte desde la app Spot&Fly (iOS/Android), por internet.
 *
 * La app del anfitrión tiene el dron conectado (Bluetooth o USB) y abre una sesión en Firestore `bfSessions/{código}`.
 * Este transporte hace de invitado, con el mismo contrato que el invitado de la app (DATA_MODEL.md de Spot&Fly):
 *   - se une con un usuario ANÓNIMO de Firebase y un apodo (sin registro): guestUid / guestName / status "active";
 *   - cada trama MSP que manda el Configurator viaja como documento `toHost` {kind:"mspReq", b64, seq, ts};
 *   - las respuestas llegan en `toGuest` {kind:"mspResp", cmd, ver, err, b64} y aquí se vuelven a empaquetar como
 *     bytes MSP para el decodificador del Configurator.
 * La seguridad (motores, reinicio, guardar en EEPROM) la decide la app del anfitrión, no este código.
 *
 * Dirección en «conexión manual»: spotandfly://CÓDIGO   (opcional: spotandfly://CÓDIGO?nick=Apodo)
 */

import { initializeApp, getApps, type FirebaseApp } from "firebase/app";
import { getAuth, signInAnonymously } from "firebase/auth";
import {
    getFirestore,
    doc,
    updateDoc,
    addDoc,
    collection,
    query,
    orderBy,
    onSnapshot,
    deleteDoc,
    serverTimestamp,
    type Firestore,
    type DocumentReference,
    type Unsubscribe,
} from "firebase/firestore";
import { SPOTANDFLY_FIREBASE_CONFIG } from "./SpotAndFlyConfig";

export interface SpotAndFlyDevice {
    path: string;
    displayName: string;
    vendorId: number;
    productId: number;
    port: number;
}

export type SpotAndFlyConnectDetail = false | { socketId: string };

type Payload = ArrayBuffer | ArrayBufferView<ArrayBuffer>;

export const SPOTANDFLY_URL = /^spotandfly:\/\/([A-Za-z0-9]{4,12})(?:\?(.*))?$/i;

/** Tiempo de espera MSP mientras dura la sesión: por internet una ida y vuelta tarda más que por cable (el
 *  Configurator usa 1000 ms y reenvía). Se mide en la prueba de concepto y se ajusta. */
const REMOTE_MSP_TIMEOUT_MS = 4000;
/** Con el anfitrión fuera de su app (sesión `hostPaused`) no llegan respuestas hasta que vuelva: se espera en vez de
 *  cortar. Cada reintento del MSP arma su temporizador con el valor vigente, así que también cubre lo ya enviado. */
const PAUSED_MSP_TIMEOUT_MS = 10 * 60 * 1000;

function bytesOf(data: Payload): Uint8Array {
    return data instanceof ArrayBuffer
        ? new Uint8Array(data)
        : new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
}

function toBase64(bytes: Uint8Array): string {
    let s = "";
    for (const b of bytes) {
        s += String.fromCharCode(b);
    }
    return btoa(s);
}

function fromBase64(b64: string): Uint8Array {
    const s = atob(b64 || "");
    const out = new Uint8Array(s.length);
    for (let i = 0; i < s.length; i++) {
        out[i] = s.charCodeAt(i);
    }
    return out;
}

function crc8DvbS2(crc: number, ch: number): number {
    crc ^= ch;
    for (let i = 0; i < 8; i++) {
        crc = crc & 0x80 ? ((crc << 1) ^ 0xd5) & 0xff : (crc << 1) & 0xff;
    }
    return crc;
}

/** Respuesta → trama MSP (v1 si cabe, si no v2, que el Configurator decodifica igual y casa por código). */
export function encodeResponse(cmd: number, ver: number, isError: boolean, payload: Uint8Array): Uint8Array {
    const dir = isError ? 0x21 /* ! */ : 0x3e; /* > */
    if (ver !== 2 && cmd <= 0xff && payload.length < 255) {
        const f = new Uint8Array(6 + payload.length);
        f.set([0x24, 0x4d, dir, payload.length, cmd]);
        let ck = payload.length ^ cmd;
        payload.forEach((b, i) => {
            f[5 + i] = b;
            ck ^= b;
        });
        f[5 + payload.length] = ck;
        return f;
    }
    const f = new Uint8Array(9 + payload.length);
    f.set([0x24, 0x58, dir, 0, cmd & 0xff, (cmd >> 8) & 0xff, payload.length & 0xff, (payload.length >> 8) & 0xff]);
    f.set(payload, 8);
    let crc = 0;
    for (let i = 3; i < 8 + payload.length; i++) {
        crc = crc8DvbS2(crc, f[i]);
    }
    f[8 + payload.length] = crc;
    return f;
}

class SpotAndFlyRemote extends EventTarget {
    connected: boolean;
    connectionInfo: null;
    bitrate: number;
    bytesSent: number;
    bytesReceived: number;
    failed: number;
    logHead: string;
    address: string;
    code: string;
    nick: string;
    private app: FirebaseApp | null;
    private db: Firestore | null;
    private sessionRef: DocumentReference | null;
    private inboxUnsub: Unsubscribe | null;
    private sessionUnsub: Unsubscribe | null;
    private txSeq: number;
    private savedTimeout: number | null;
    /** Para la medición de la prueba: momento de envío de cada petición por código MSP. */
    private sentAt: Map<number, number[]>;
    rtts: number[];
    private hostPaused: boolean;
    private pauseDialogId: number | null;

    constructor() {
        super();
        this.connected = false;
        this.connectionInfo = null;
        this.bitrate = 0;
        this.bytesSent = 0;
        this.bytesReceived = 0;
        this.failed = 0;
        this.logHead = "[SPOTANDFLY]";
        this.address = "";
        this.code = "";
        this.nick = "";
        this.app = null;
        this.db = null;
        this.sessionRef = null;
        this.inboxUnsub = null;
        this.sessionUnsub = null;
        this.txSeq = 0;
        this.savedTimeout = null;
        this.sentAt = new Map();
        this.rtts = [];
        this.hostPaused = false;
        this.pauseDialogId = null;
        this.connect = this.connect.bind(this);
    }

    createPort(url: string): SpotAndFlyDevice {
        this.address = url;
        return this.getConnectedDevice();
    }

    getConnectedDevice(): SpotAndFlyDevice {
        return { path: this.address, displayName: "Spot&Fly remoto", vendorId: 0, productId: 0, port: 0 };
    }

    async getDevices(): Promise<SpotAndFlyDevice[]> /* NOSONAR: implements the async SerialProtocol interface */ {
        return [];
    }

    private fail(why: string, e?: unknown): void {
        console.error(`${this.logHead} ${why}`, e ?? "");
        this.dispatchEvent(new CustomEvent<SpotAndFlyConnectDetail>("connect", { detail: false }));
    }

    private askNick(fromUrl: string | null): string {
        let nick = (fromUrl || "").trim();
        if (!nick) {
            try {
                nick = localStorage.getItem("spotandfly_nick") || "";
            } catch {
                /* sin almacenamiento */
            }
        }
        let answer: string | null;
        try {
            answer = window.prompt("Tu apodo para esta sesión (lo verá quien comparte el dron):", nick || "");
        } catch {
            // Navegadores sin prompt() (medido 11/11 en el navegador integrado): sin esto la conexión se rompía
            answer = nick || "Invitado";
        }
        nick = (answer ?? "").trim().slice(0, 24);
        if (nick) {
            try {
                localStorage.setItem("spotandfly_nick", nick);
            } catch {
                /* sin almacenamiento */
            }
        }
        return nick;
    }

    async connect(path: string): Promise<void> /* NOSONAR: implements the async SerialProtocol interface */ {
        this.address = path;
        const m = SPOTANDFLY_URL.exec(path.trim());
        if (!m) {
            this.fail(`Dirección no válida: ${path} (usa spotandfly://CÓDIGO)`);
            return;
        }
        this.code = m[1].toUpperCase();
        const params = new URLSearchParams(m[2] || "");
        this.nick = this.askNick(params.get("nick"));
        if (!this.nick) {
            this.fail("Sin apodo: conexión cancelada");
            return;
        }

        try {
            this.app =
                getApps().find((a) => a.name === "spotandfly") ??
                initializeApp(SPOTANDFLY_FIREBASE_CONFIG, "spotandfly");
            const auth = getAuth(this.app);
            const cred = auth.currentUser ? { user: auth.currentUser } : await signInAnonymously(auth);
            const uid = cred.user.uid;
            this.db = getFirestore(this.app);
            this.sessionRef = doc(this.db, "bfSessions", this.code);
            await this.claimSlot(uid, 4);
        } catch (e) {
            this.fail(`No se pudo unir a la sesión ${this.code}`, e);
            return;
        }

        // Respuestas del anfitrión → bytes MSP para el Configurator
        const inbox = query(collection(this.sessionRef, "toGuest"), orderBy("seq"));
        // La primera foto trae lo que ya había en la cola: respuestas a un invitado anterior que se fue antes de
        // recibirlas (medido 11/11: un ERROR de MSP2_GET_TEXT del intento previo llegó al unirse). Se borran sin
        // pasarlas al Configurator, y no se da la conexión por hecha (ni se pide nada) hasta tenerla.
        let firstInbox = true;
        let inboxFailed = false;
        let inboxReady: () => void = () => undefined;
        const inboxReadyPromise = new Promise<void>((resolve) => {
            inboxReady = resolve;
        });
        this.inboxUnsub = onSnapshot(
            inbox,
            (snap) => {
                if (firstInbox) {
                    firstInbox = false;
                    if (!snap.empty) {
                        console.log(`${this.logHead} ${snap.size} respuestas viejas en la cola: descartadas`);
                    }
                    snap.docs.forEach((d) => void deleteDoc(d.ref).catch(() => undefined));
                    inboxReady();
                    return;
                }
                const added = snap
                    .docChanges()
                    .filter((c) => c.type === "added")
                    .sort((a, b) => (a.doc.get("seq") ?? 0) - (b.doc.get("seq") ?? 0));
                for (const c of added) {
                    const d = c.doc.data();
                    if (d.kind === "mspResp") {
                        const cmd = Number(d.cmd) || 0;
                        const frame = encodeResponse(
                            cmd,
                            Number(d.ver) || 1,
                            d.err === true,
                            fromBase64(String(d.b64 || "")),
                        );
                        this.noteRtt(cmd);
                        this.bytesReceived += frame.byteLength;
                        this.dispatchEvent(new CustomEvent<Uint8Array>("receive", { detail: frame }));
                    }
                    void deleteDoc(c.doc.ref).catch(() => undefined);
                }
            },
            (e) => {
                // Antes se tragaba en silencio en la app y el invitado se quedaba sin respuestas: aquí, a la consola.
                console.error(`${this.logHead} Escucha de toGuest falló`, e);
                if (firstInbox) {
                    inboxFailed = true;
                    inboxReady();
                    return;
                }
                void this.closeFromRemote();
            },
        );

        // Fin de sesión por el anfitrión («Cortar»)
        this.sessionUnsub = onSnapshot(this.sessionRef, (snap) => {
            void this.applyHostPaused(snap.get("hostPaused") === true);
            if (snap.get("status") === "ended") {
                console.log(`${this.logHead} El anfitrión cortó la sesión`);
                void this.closeFromRemote();
            }
        });

        await inboxReadyPromise;
        if (inboxFailed) {
            this.sessionUnsub?.();
            this.inboxUnsub?.();
            this.inboxUnsub = this.sessionUnsub = null;
            // Ya se había ocupado el hueco: que el anfitrión lo reabra
            await this.sendToHost({ kind: "guestLeaving" }).catch(() => undefined);
            this.fail(`No se pudo escuchar la sesión ${this.code}`);
            return;
        }
        await this.setMspTimeout(this.hostPaused ? PAUSED_MSP_TIMEOUT_MS : REMOTE_MSP_TIMEOUT_MS);
        this.connected = true;
        console.log(`${this.logHead} Unido a ${this.code} como «${this.nick}»`);
        if (this.hostPaused) {
            // El anfitrión ya estaba fuera de su app al unirse: que salga la ventana de espera igualmente
            this.hostPaused = false;
            void this.applyHostPaused(true);
        }
        this.dispatchEvent(new CustomEvent<SpotAndFlyConnectDetail>("connect", { detail: { socketId: this.address } }));
    }

    /** Las reglas solo dejan unirse si guestUid estaba vacío; al reintentar justo tras salir, el anfitrión puede no
     *  haber reabierto el hueco aún (igual que el invitado de la app: 4 intentos con 0,8 s). */
    private async claimSlot(uid: string, attempts: number): Promise<void> {
        for (let i = 0; i < attempts; i++) {
            try {
                await updateDoc(this.sessionRef!, {
                    guestUid: uid,
                    guestName: this.nick,
                    status: "active",
                    updatedAt: serverTimestamp(),
                });
                return;
            } catch (e) {
                if (i === attempts - 1) {
                    throw e;
                }
                await new Promise((r) => setTimeout(r, 800));
            }
        }
    }

    /** El anfitrión salió o volvió a su app: alarga o restaura la espera del MSP y lo avisa en el registro. */
    private async applyHostPaused(paused: boolean): Promise<void> {
        if (paused === this.hostPaused) {
            return;
        }
        this.hostPaused = paused;
        if (this.connected) {
            await this.setMspTimeout(paused ? PAUSED_MSP_TIMEOUT_MS : REMOTE_MSP_TIMEOUT_MS);
        }
        // Import dinámico, como MSP: evita el ciclo serial.ts → este fichero → ...
        const [{ gui_log }, { i18n }, { useDialogStore }] = await Promise.all([
            import("../gui_log"),
            import("../localization"),
            import("../../stores/dialog"),
        ]);
        gui_log(i18n.getMessage(paused ? "spotandflyHostPaused" : "spotandflyHostBack"));
        // A la vista (Pablo 11/11: en el registro no se veía): ventana de espera que se cierra sola al volver el
        // anfitrión, con «Desconectar» por si no se quiere esperar.
        const dialogs = useDialogStore();
        if (paused && this.connected) {
            dialogs.open(
                "WaitDialog",
                {
                    title: i18n.getMessage("spotandflyHostPaused"),
                    showCancel: true,
                    cancelText: i18n.getMessage("disconnect"),
                },
                {
                    cancel: () => {
                        dialogs.close();
                        this.pauseDialogId = null;
                        void import("../serial_backend").then(({ connectDisconnect }) => connectDisconnect());
                    },
                },
            );
            this.pauseDialogId = dialogs.activeDialog?.id ?? null;
        } else if (!paused && this.pauseDialogId !== null) {
            if (dialogs.activeDialog?.id === this.pauseDialogId) {
                dialogs.close();
            }
            this.pauseDialogId = null;
        }
    }

    private async setMspTimeout(ms: number | null): Promise<void> {
        // Import dinámico: msp.ts importa serial.ts, que importa este fichero.
        const { default: MSP } = await import("../msp");
        if (ms === null) {
            if (this.savedTimeout !== null) {
                MSP.TIMEOUT = this.savedTimeout;
            }
            this.savedTimeout = null;
        } else {
            this.savedTimeout ??= MSP.TIMEOUT;
            MSP.TIMEOUT = ms;
        }
    }

    private noteRtt(cmd: number): void {
        const q = this.sentAt.get(cmd);
        const t0 = q?.shift();
        if (t0 !== undefined) {
            const rtt = performance.now() - t0;
            this.rtts.push(rtt);
            if (this.rtts.length % 20 === 0) {
                const s = [...this.rtts].sort((a, b) => a - b);
                const p = (x: number) => s[Math.min(s.length - 1, Math.floor(x * s.length))].toFixed(0);
                console.log(
                    `${this.logHead} RTT ${s.length} respuestas: mediana ${p(0.5)} ms · p90 ${p(0.9)} ms · máx ${p(1)} ms`,
                );
            }
        }
    }

    private noteSend(bytes: Uint8Array): void {
        let cmd = -1;
        if (bytes[0] === 0x24 && bytes[1] === 0x4d) {
            cmd = bytes[4];
        } else if (bytes[0] === 0x24 && bytes[1] === 0x58) {
            cmd = bytes[4] | (bytes[5] << 8);
        }
        if (cmd >= 0) {
            const q = this.sentAt.get(cmd) ?? [];
            q.push(performance.now());
            this.sentAt.set(cmd, q);
        }
    }

    private async sendToHost(fields: Record<string, unknown>): Promise<void> {
        if (!this.sessionRef) {
            return;
        }
        this.txSeq += 1;
        await addDoc(collection(this.sessionRef, "toHost"), { ...fields, seq: this.txSeq, ts: serverTimestamp() });
    }

    private async closeFromRemote(): Promise<void> {
        if (!this.connected) {
            return;
        }
        await this.disconnect();
        this.dispatchEvent(new CustomEvent("disconnect", { detail: { socketId: this.address } }));
    }

    async disconnect(): Promise<void> /* NOSONAR: implements the async SerialProtocol interface */ {
        const wasConnected = this.connected;
        this.connected = false;
        this.inboxUnsub?.();
        this.sessionUnsub?.();
        this.inboxUnsub = this.sessionUnsub = null;
        if (wasConnected) {
            // Avisa al anfitrión para que reabra el hueco (igual que «Salir» en la app)
            await this.sendToHost({ kind: "guestLeaving" }).catch(() => undefined);
        }
        await this.setMspTimeout(null);
        this.hostPaused = false;
        if (this.pauseDialogId !== null) {
            const { useDialogStore } = await import("../../stores/dialog");
            const dialogs = useDialogStore();
            if (dialogs.activeDialog?.id === this.pauseDialogId) {
                dialogs.close();
            }
            this.pauseDialogId = null;
        }
        this.sessionRef = null;
        this.bytesReceived = 0;
        this.bytesSent = 0;
        this.sentAt.clear();
    }

    async /* NOSONAR: implements the async SerialProtocol interface */ send(
        data: Payload,
        cb?: (result: { error: unknown; bytesSent: number }) => void,
    ): Promise<{ bytesSent: number }> {
        if (!this.connected || !this.sessionRef) {
            cb?.({ error: null, bytesSent: 0 });
            return { bytesSent: 0 };
        }
        const bytes = bytesOf(data);
        try {
            this.noteSend(bytes);
            // No se espera a Firestore: el MSP del Configurator cuenta como enviado al encolar (igual que un socket).
            void this.sendToHost({ kind: "mspReq", b64: toBase64(bytes) }).catch((e) =>
                console.error(`${this.logHead} Envío falló`, e),
            );
            this.bytesSent += bytes.byteLength;
            cb?.({ error: null, bytesSent: bytes.byteLength });
            return { bytesSent: bytes.byteLength };
        } catch (e) {
            cb?.({ error: e, bytesSent: 0 });
            return { bytesSent: 0 };
        }
    }
}

export default SpotAndFlyRemote;
