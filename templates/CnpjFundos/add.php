<?php
/**
 * @var \App\View\AppView $this
 * @var \App\Model\Entity\CnpjFundo $cnpjFundo
 */
?>
<div class="row">
    <aside class="column">
        <div class="side-nav">
            <h4 class="heading"><?= __('Actions') ?></h4>
            <?= $this->Html->link(__('List Cnpj Fundos'), ['action' => 'index'], ['class' => 'side-nav-item']) ?>
        </div>
    </aside>
    <div class="column-responsive column-80">
        <div class="cnpjFundos form content">
            <?= $this->Form->create($cnpjFundo) ?>
            <fieldset>
                <legend><?= __('Add Cnpj Fundo') ?></legend>
                <?php
                    echo $this->Form->control('CNPJ');
                    echo $this->Form->control('DENOM_SOCIAL');
                    echo $this->Form->control('DT_REG_CVM');
                ?>
            </fieldset>
            <?= $this->Form->button(__('Submit')) ?>
            <?= $this->Form->end() ?>
        </div>
    </div>
</div>
