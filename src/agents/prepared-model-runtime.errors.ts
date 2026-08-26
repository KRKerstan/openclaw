export class PreparedModelRuntimeOwnerNotPublishedError extends Error {}

export class PreparedModelRuntimePublicationSupersededError extends PreparedModelRuntimeOwnerNotPublishedError {}

export class PreparedModelCatalogGenerationInvalidError extends Error {
  override readonly name = "PreparedModelCatalogGenerationInvalidError";
}
