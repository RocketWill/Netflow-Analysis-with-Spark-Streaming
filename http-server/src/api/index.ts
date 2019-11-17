import * as express from 'express';
import { accessTrendRouter } from './routes';

const api = express();
// You may add api specific middlewares here
// TODO: move all controllers in the src/api/controllers folder
// api.get('/', (req, res) => {
//   res.send({
//     message: 'Hello from the API',
//   });
// });

api.use('/access-trend', accessTrendRouter)

export default api;