// Libraries
import React, {PureComponent} from 'react'
// Components
import TelegrafInstructions from 'src/onboarding/components/verifyStep/TelegrafInstructions'
import CreateOrUpdateConfig from 'src/onboarding/components/verifyStep/CreateOrUpdateConfig'
import DataListening from 'src/onboarding/components/verifyStep/DataListening'
// Decorator
import {ErrorHandling} from 'src/shared/decorators/errors'
// Types
import {NotificationAction} from 'src/types'

interface Props {
  notify: NotificationAction
  bucket: string
  org: string
  configID: string
  authToken: string
}

@ErrorHandling
class DataStreaming extends PureComponent<Props> {
  public render() {
    const {authToken, org, configID, bucket, notify} = this.props

    return (
      <>
        <CreateOrUpdateConfig org={org} authToken={authToken}>
          {() => (
            <TelegrafInstructions
              notify={notify}
              authToken={authToken}
              configID={configID}
            />
          )}
        </CreateOrUpdateConfig>

        <DataListening bucket={bucket} />
      </>
    )
  }
}

export default DataStreaming
