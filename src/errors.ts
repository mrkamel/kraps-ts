export class KrapsError extends Error {
  constructor(message?: string) {
    super(message);
    this.name = 'KrapsError';
  }
}

export class InvalidAction extends KrapsError {
  constructor(message?: string) {
    super(message);
    this.name = 'InvalidAction';
  }
}

export class InvalidStep extends KrapsError {
  constructor(message?: string) {
    super(message);
    this.name = 'InvalidStep';
  }
}

export class InvalidJob extends KrapsError {
  constructor(message?: string) {
    super(message);
    this.name = 'InvalidJob';
  }
}

export class JobStopped extends KrapsError {
  constructor(message?: string) {
    super(message);
    this.name = 'JobStopped';
  }
}

export class IncompatibleFrame extends KrapsError {
  constructor(message?: string) {
    super(message);
    this.name = 'IncompatibleFrame';
  }
}

export class InvalidChunkLimit extends KrapsError {
  constructor(message?: string) {
    super(message);
    this.name = 'InvalidChunkLimit';
  }
}
