import { Router } from 'express';
import { getAccessTrend } from '../controllers/accessTrend';
export const accessTrendRouter = Router();
accessTrendRouter.get('/', getAccessTrend);
export default accessTrendRouter;