// Libraries
import React, {PureComponent} from 'react'
// Components
import InitStep from 'src/onboarding/components/InitStep'
import AdminStep from 'src/onboarding/components/AdminStep'
import CompletionStep from 'src/onboarding/components/CompletionStep'
import {ErrorHandling} from 'src/shared/decorators/errors'
// Types
import {SetupParams} from 'src/onboarding/apis'
import {OnboardingStepProps} from 'src/onboarding/containers/OnboardingWizard'
import {setupAdmin} from 'src/onboarding/actions'

interface Props {
  onboardingStepProps: OnboardingStepProps
  setupParams: SetupParams
  currentStepIndex: number
  onSetupAdmin: typeof setupAdmin
  orgID: string
  bucketID: string
}

@ErrorHandling
class OnboardingStepSwitcher extends PureComponent<Props> {
  public render() {
    const {
      currentStepIndex,
      orgID,
      bucketID,
      onboardingStepProps,
      onSetupAdmin,
    } = this.props

    switch (currentStepIndex) {
      case 0:
        return <InitStep {...onboardingStepProps} />
      case 1:
        return (
          <AdminStep {...onboardingStepProps} onSetupAdmin={onSetupAdmin} />
        )
      case 2:
        return (
          <CompletionStep
            {...onboardingStepProps}
            orgID={orgID}
            bucketID={bucketID}
          />
        )
      default:
        return <div />
    }
  }
}

export default OnboardingStepSwitcher
