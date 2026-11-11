/*
 * Configuración web de Firebase del proyecto Spot&Fly (app web registrada en el proyecto). Una clave de app web de
 * Firebase es pública por diseño: lo que protege los datos son las reglas de Firestore (bfSessions admite invitados
 * anónimos; nada más).
 */
export const SPOTANDFLY_FIREBASE_CONFIG = {
    apiKey: import.meta.env.VITE_SPOTANDFLY_API_KEY as string,
    authDomain: "spotitfy-71f61.firebaseapp.com",
    projectId: "spotitfy-71f61",
    appId: import.meta.env.VITE_SPOTANDFLY_APP_ID as string,
};
