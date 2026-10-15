const { z } = require('zod');

function validateBody(schema) {
  return (req, res, next) => {
    const result = schema.safeParse(req.body);
    if (!result.success) {
      const formattedErrors = result.error.errors.map((e) => ({
        path: e.path.join('.'),
        message: e.message,
      }));
      return res.status(400).json({
        error: 'Bad Request',
        message: 'Validation failed on request body.',
        details: formattedErrors,
      });
    }
    req.validatedBody = result.data;
    next();
  };
}

const createTicketSchema = z.object({
  title: z.string().trim().min(3, 'Title must be at least 3 characters').max(200, 'Title cannot exceed 200 characters'),
  description: z.string().trim().min(5, 'Description must be at least 5 characters'),
  customer: z.object({
    name: z.string().trim().optional().default('Anonymous Customer'),
    email: z.string().trim().email('A valid customer email address is required'),
  }),
  channel: z.enum(['API', 'PORTAL', 'EMAIL', 'CHAT', 'WEBHOOK']).optional().default('API'),
  metadata: z.record(z.any()).optional().default({}),
});

const loginSchema = z.object({
  email: z.string().trim().email('A valid email address is required'),
  password: z.string().min(6, 'Password must be at least 6 characters'),
});

module.exports = {
  validateBody,
  createTicketSchema,
  loginSchema,
};
