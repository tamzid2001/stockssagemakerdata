/** Node only. Reuses the private credentials written by `quantura login`. */
export function accessToken(baseUrl?: string): Promise<string>;
export function login(options?: {noBrowser?: boolean}): Promise<void>;
export function logout(): Promise<boolean>;
export function credentialFile(): string;
export function pkce(): {verifier: string; challenge: string; state: string};
export function validCallback(url: URL, state: string): boolean;
