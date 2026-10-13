export class HttpError extends Error { constructor(status, message) { super(message); this.status = status; } }
export const asyncRoute = handler => (req, res, next) => Promise.resolve(handler(req, res, next)).catch(next);
export function errorHandler(error, req, res, next) {
  if (res.headersSent) return next(error);
  const status = error.status || (['22P02','23514','22001'].includes(error.code) ? 400 : error.code==='23505' ? 409 : ['ECONNREFUSED','57P01','08006','53300'].includes(error.code) ? 503 : 500);
  if (status >= 500) console.error('Request failed:', req.method, req.path, error.code || error.name);
  res.status(status).json({ success: false, error: !error.status && status>=500 ? 'The server could not complete this request' : !error.status && status===409 ? 'A conflicting record already exists' : !error.status && status===400 ? 'Invalid request data' : error.message });
}
