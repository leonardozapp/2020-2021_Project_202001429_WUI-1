<?php
/**
 * @var \App\View\AppView $this
 * @var \App\Model\Entity\CadastroFundo $cadastroFundo
 */
?>
<div class="row">
    <aside class="column">
        <div class="side-nav">
            <h4 class="heading"><?= __('Actions') ?></h4>
            <?= $this->Html->link(__('List Cadastro Fundos'), ['action' => 'index'], ['class' => 'side-nav-item']) ?>
        </div>
    </aside>
    <div class="column-responsive column-80">
        <div class="cadastroFundos form content">
            <?= $this->Form->create($cadastroFundo) ?>
            <fieldset>
                <legend><?= __('Add Cadastro Fundo') ?></legend>
                <?php
                    echo $this->Form->control('DT_CONST');
                    echo $this->Form->control('DT_REG_CVM');
                ?>
            </fieldset>
            <?= $this->Form->button(__('Submit')) ?>
            <?= $this->Form->end() ?>
        </div>
    </div>
</div>
