import * as express from 'express';
import { Request, Response } from 'express';
import api from './api/index';
const cors = require('cors')
const app = express();
const server = require('http').createServer(app);

const { 
  PORT = 3088
} = process.env;

app.use(cors({credentials: true, origin: true}))


// app.get('/', (req: Request, res: Response) => {
//   res.send({
//     message: 'hello world!',
//   })
// });

app.use('/api/v1', api);

if (require.main === module) {
  app.listen(PORT, () => {
    console.log('server started at http://localhost:'+PORT);
  });
}

export default app;
